#!/bin/bash
set -euo pipefail

# ================= CONFIG =================
N=${1:-8}  # number of MPI ranks / workers
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
CONTROLLER="./test2.py"

RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

RESERVED_CORE=31  # never touch (also excluded from selection)

# 3 steps (kHz)
FREQ_HIGH=2000000
FREQ_MID=1600000
FREQ_LOW=1200000

OUTDIR="run_$(date +%Y%m%d_%H%M%S)_n${N}"
mkdir -p "$OUTDIR"

MARKER="$OUTDIR/phase_marker.txt"

CTRL_LOG="$OUTDIR/controller.log"
BASE_LOG="$OUTDIR/baseline.log"
CTRL_RUN_LOG="$OUTDIR/controlled.log"
RANKFILE="$OUTDIR/host_rankfile"

MON_PID=""

cleanup() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "${MON_PID}" 2>/dev/null; then break; fi
      sleep 0.1
    done
    if kill -0 "${MON_PID}" 2>/dev/null; then
      kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    fi
    wait "${MON_PID}" 2>/dev/null || true
    MON_PID=""
  fi
}
trap cleanup INT TERM EXIT

# ================= HELPERS =================
read_energy_uj() { cat "$RAPL_FILE" 2>/dev/null || echo "0"; }
read_max_uj() { cat "$MAX_RAPL_FILE" 2>/dev/null || echo "262143328850"; }

energy_diff_uj() {
  local before=$1 after=$2 maxv=$3
  local diff=$(( after - before ))
  if (( diff < 0 )); then diff=$(( diff + maxv )); fi
  echo "$diff"
}

now_ns() { date +%s%N; }
get_cpuset_str() { taskset -cp $$ | sed 's/.*: //'; }

expand_core_list() {
  local s="$1"
  python3 - "$s" <<'PY'
import sys
s=sys.argv[1].strip()
cores=set()
if s:
  for part in s.split(","):
    part=part.strip()
    if not part: continue
    if "-" in part:
      a,b=part.split("-",1)
      a=int(a); b=int(b)
      if b<a: a,b=b,a
      for c in range(a,b+1): cores.add(c)
    else:
      cores.add(int(part))
print(" ".join(map(str, sorted(cores))))
PY
}

pick_layout() {
  local n="$1"
  local cpuset_str expanded
  cpuset_str="$(get_cpuset_str)"
  expanded="$(expand_core_list "$cpuset_str")"

  python3 - "$expanded" "$n" "$RESERVED_CORE" <<'PY'
import sys
expanded=sys.argv[1].strip()
n=int(sys.argv[2])
reserved=int(sys.argv[3])

avail=[int(x) for x in expanded.split()] if expanded else []
avail=[c for c in avail if c != reserved]

if len(avail) < n+1:
  sys.stderr.write(f"ERROR: Need at least {n+1} cores in cpuset (N workers + 1 monitor).\n")
  sys.stderr.write(f"  Have {len(avail)} cores (after removing reserved core {reserved}).\n")
  sys.exit(2)

workers=avail[:n]
wset=set(workers)
mon=max([c for c in avail if c not in wset])
print(",".join(map(str,workers)) + "|" + str(mon))
PY
}

make_rankfile_from_workers() {
  local worker_csv="$1"
  : > "$RANKFILE"
  local r=0
  IFS=',' read -r -a CORES <<< "$worker_csv"
  for core in "${CORES[@]}"; do
    echo "rank $r=localhost slot=$core" >> "$RANKFILE"
    r=$((r+1))
  done
}

reset_governors_perf_list() {
  local worker_csv="$1"
  IFS=',' read -r -a CORES <<< "$worker_csv"
  for c in "${CORES[@]}"; do
    echo "performance" > "/sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor" 2>/dev/null || true
  done
}

start_controller() {
  local monitor_core="$1"
  local worker_csv="$2"
  local rank0_core="$3"

  : > "$CTRL_LOG"
  : > "$MARKER"

  taskset -c "$monitor_core" python3 -u "$CONTROLLER" \
    --worker-cores "$worker_csv" \
    --rank0-core "$rank0_core" \
    --marker "$MARKER" \
    --freq-high "$FREQ_HIGH" --freq-mid "$FREQ_MID" --freq-low "$FREQ_LOW" \
    --heartbeat \
    > "$CTRL_LOG" 2>&1 &

  MON_PID=$!
  sleep 0.3
}

stop_controller_hard() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "${MON_PID}" 2>/dev/null; then break; fi
      sleep 0.1
    done
    if kill -0 "${MON_PID}" 2>/dev/null; then
      kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    fi
    wait "${MON_PID}" 2>/dev/null || true
  fi
  MON_PID=""
}

parse_ctrl_summary() {
  local logfile="$1"
  # outputs: "<wall_s> <energy_j>"
  if grep -q "CTRL_SUMMARY" "$logfile"; then
    local wall energy
    wall=$(grep "CTRL_SUMMARY" "$logfile" | tail -n1 | sed -n 's/.*wall_s=\([0-9.]\+\).*/\1/p')
    energy=$(grep "CTRL_SUMMARY" "$logfile" | tail -n1 | sed -n 's/.*energy_j=\([0-9.]\+\).*/\1/p')
    echo "${wall:-0} ${energy:-0}"
  else
    echo "0 0"
  fi
}

run_case() {
  local label="$1"          # BASELINE or CONTROLLED
  local log="$2"
  local use_controller="$3" # 0/1
  local monitor_core="$4"
  local worker_csv="$5"
  local rank0_core="$6"

  : > "$MARKER"

  if [[ "$use_controller" == "1" ]]; then
    start_controller "$monitor_core" "$worker_csv" "$rank0_core"
  else
    MON_PID=""
    : > "$CTRL_LOG"
  fi

  local max_uj before_uj after_uj diff_uj
  local start_ns end_ns
  max_uj="$(read_max_uj)"
  before_uj="$(read_energy_uj)"
  start_ns="$(now_ns)"

  set +e
  PHASE_MARKER_PATH="$MARKER" mpirun -np "$N" \
    --rankfile "$RANKFILE" \
    --report-bindings \
    "$BINARY" -i "$INPUT" > "$log" 2>&1
  local mpistatus=$?
  set -e

  end_ns="$(now_ns)"
  after_uj="$(read_energy_uj)"

  # Stop controller AFTER we read after_uj so miniMD energy isn't polluted by cleanup
  if [[ "$use_controller" == "1" ]]; then
    stop_controller_hard
  fi

  diff_uj="$(energy_diff_uj "$before_uj" "$after_uj" "$max_uj")"

  local wall_s energy_j
  wall_s="$(echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc -l)"
  energy_j="$(echo "scale=6; $diff_uj / 1000000" | bc -l)"

  if [[ $mpistatus -ne 0 ]]; then
    echo "ERROR: mpirun exited with status $mpistatus. See log: $log" >&2
    return $mpistatus
  fi

  echo "$wall_s $energy_j"
}

# ================= MAIN =================
echo "=========================================================="
echo "miniMD + frequency-controller evaluation (no heaters)"
echo "Workers (MPI ranks): $N"
echo "Output dir: $OUTDIR"
echo "Marker file: $MARKER"
echo "=========================================================="

[[ -f "$BINARY" ]] || { echo "ERROR: $BINARY not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: $INPUT not found"; exit 1; }
[[ -f "$CONTROLLER" ]] || { echo "ERROR: $CONTROLLER not found"; exit 1; }

CPUSET_STR="$(get_cpuset_str)"
CPUSET_EXPANDED="$(expand_core_list "$CPUSET_STR")"
echo "[INFO] taskset cpuset: $CPUSET_STR"
echo "[INFO] expanded cores: $CPUSET_EXPANDED"

LAYOUT="$(pick_layout "$N")"
WORKER_CORES="$(echo "$LAYOUT" | cut -d'|' -f1)"
MONITOR_CORE="$(echo "$LAYOUT" | cut -d'|' -f2)"
RANK0_CORE="$(echo "$WORKER_CORES" | cut -d',' -f1)"

echo "[INFO] Worker cores: $WORKER_CORES"
echo "[INFO] Rank0 core:   $RANK0_CORE"
echo "[INFO] Monitor core: $MONITOR_CORE (auto-picked)"
echo "[INFO] Reserved core $RESERVED_CORE will not be touched."

make_rankfile_from_workers "$WORKER_CORES"

reset_governors_perf_list "$WORKER_CORES"
echo ""
echo "1) BASELINE (no controller)"
read base_time base_energy <<< "$(run_case "BASELINE" "$BASE_LOG" 0 "$MONITOR_CORE" "$WORKER_CORES" "$RANK0_CORE")"
echo "   Baseline:   ${base_time}s | ${base_energy}J"

reset_governors_perf_list "$WORKER_CORES"
echo ""
echo "2) CONTROLLED (controller active)"
read ctrl_time ctrl_energy <<< "$(run_case "CONTROLLED" "$CTRL_RUN_LOG" 1 "$MONITOR_CORE" "$WORKER_CORES" "$RANK0_CORE")"
echo "   Controlled: ${ctrl_time}s | ${ctrl_energy}J"

read ctrl_overhead_s ctrl_overhead_j <<< "$(parse_ctrl_summary "$CTRL_LOG")"

reset_governors_perf_list "$WORKER_CORES" || true

echo ""
if (( $(echo "$base_time > 0" | bc -l) )); then
  time_pct=$(echo "scale=4; (($ctrl_time - $base_time) / $base_time) * 100" | bc -l)
  saved_j=$(echo "scale=6; $base_energy - $ctrl_energy" | bc -l)
  energy_pct=$(echo "scale=4; (($base_energy - $ctrl_energy) / $base_energy) * 100" | bc -l)
  net_saved_j=$(echo "scale=6; ($base_energy - $ctrl_energy) - $ctrl_overhead_j" | bc -l)

  echo "================ RESULTS ================"
  printf "Runtime Impact: %+.2f%%\n" "$time_pct"
  printf "Energy Savings (miniMD only): %s J (%.2f%%)\n" "$saved_j" "$energy_pct"
  echo ""
  echo "============= CONTROLLER OVERHEAD ============="
  echo "Controller wall time: ${ctrl_overhead_s}s"
  echo "Controller energy:    ${ctrl_overhead_j}J"
  echo "Net energy saved (minus controller): ${net_saved_j} J"
  echo ""
  echo "Logs:"
  echo "  Baseline:    $BASE_LOG"
  echo "  Controlled:  $CTRL_RUN_LOG"
  echo "  Controller:  $CTRL_LOG"
  echo "  Rankfile:    $RANKFILE"
else
  echo "ERROR: Baseline time was 0"
fi
