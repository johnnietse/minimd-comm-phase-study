#!/bin/bash
set -euo pipefail

# ================= CONFIG =================
N=${1:-8}  # number of MPI ranks / workers
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
CONTROLLER="./test.py"   # monitor script

RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

RESERVED_CORE=31  # never touch

# Make OUTDIR absolute to avoid any cwd surprises
OUTDIR="$(pwd)/run_$(date +%Y%m%d_%H%M%S)_n${N}"
mkdir -p "$OUTDIR"

RANKFILE="$OUTDIR/host_rankfile"
MON_PID=""

cleanup() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    sleep 0.2
    kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    wait "${MON_PID}" 2>/dev/null || true
    MON_PID=""
  fi
}
trap cleanup INT TERM EXIT

# ================= HELPERS =================
read_energy_uj() { cat "$RAPL_FILE" 2>/dev/null || echo "0"; }
read_max_uj() { cat "$MAX_RAPL_FILE" 2>/dev/null || echo "65532610987"; }

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
    if not part:
      continue
    if "-" in part:
      a,b=part.split("-",1)
      a=int(a); b=int(b)
      if b < a: a,b=b,a
      for c in range(a,b+1):
        cores.add(c)
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
candidates=[c for c in avail if c not in wset]
if not candidates:
  sys.stderr.write("ERROR: No spare core for monitor after choosing workers.\n")
  sys.exit(3)

mon=max(candidates)
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
  local case_dir="$4"

  mkdir -p "$case_dir"

  # IMPORTANT: these are *filenames* because we cd into case_dir
  local ctrl_log="controller.log"
  local ctrl_csv="controller.csv"
  local marker="phase_marker.txt"

  : > "$case_dir/$ctrl_log"
  : > "$case_dir/$marker"

  (
    cd "$case_dir"
    taskset -c "$monitor_core" python3 -u "$CONTROLLER" \
      --worker-cores "$worker_csv" \
      --rank0-core "$rank0_core" \
      --marker "$marker" \
      --log "$ctrl_csv" \
      > "$ctrl_log" 2>&1
  ) &

  MON_PID=$!
  sleep 0.2
}

stop_controller_hard() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    sleep 0.2
    kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    wait "${MON_PID}" 2>/dev/null || true
  fi
  MON_PID=""
}

# Prints ONLY: "wall_s energy_j" on success
run_case() {
  local label="$1"          # BASELINE or CONTROLLED
  local use_controller="$2" # 0/1
  local monitor_core="$3"
  local worker_csv="$4"
  local rank0_core="$5"
  local bin_abs="$6"
  local input_abs="$7"

  local case_dir="$OUTDIR/${label,,}"
  mkdir -p "$case_dir"

  # filenames (because we cd case_dir)
  local mpilog="${label,,}.log"

  if [[ "$use_controller" == "1" ]]; then
    start_controller "$monitor_core" "$worker_csv" "$rank0_core" "$case_dir"
  else
    MON_PID=""
    : > "$case_dir/phase_marker.txt"
  fi

  local max_uj before_uj after_uj diff_uj
  local start_ns end_ns
  max_uj="$(read_max_uj)"
  before_uj="$(read_energy_uj)"
  start_ns="$(now_ns)"

  set +e
  (
    cd "$case_dir"
    mpirun -np "$N" \
      --rankfile "$RANKFILE" \
      --report-bindings \
      "$bin_abs" -i "$input_abs" > "$mpilog" 2>&1
  )
  local mpistatus=$?
  set -e

  end_ns="$(now_ns)"
  after_uj="$(read_energy_uj)"

  stop_controller_hard

  diff_uj="$(energy_diff_uj "$before_uj" "$after_uj" "$max_uj")"

  local wall_s energy_j
  wall_s="$(echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc -l)"
  energy_j="$(echo "scale=6; $diff_uj / 1000000" | bc -l)"

  if [[ $mpistatus -ne 0 ]]; then
    echo "ERROR: mpirun exited with status $mpistatus. See log: $case_dir/$mpilog" >&2
    return $mpistatus
  fi

  echo "$wall_s $energy_j"
}

# ================= MAIN =================
echo "=========================================================="
echo "miniMD + phase-aware monitor evaluation (non-sequential OK)"
echo "Workers (MPI ranks): $N"
echo "Output dir: $OUTDIR"
echo "=========================================================="

[[ -f "$BINARY" ]] || { echo "ERROR: $BINARY not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: $INPUT not found"; exit 1; }
[[ -f "$CONTROLLER" ]] || { echo "ERROR: $CONTROLLER not found"; exit 1; }

BIN_ABS="$(readlink -f "$BINARY")"
INPUT_ABS="$(readlink -f "$INPUT")"
CONTROLLER="$(readlink -f "$CONTROLLER")"

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
base_out="$(run_case "BASELINE" 0 "$MONITOR_CORE" "$WORKER_CORES" "$RANK0_CORE" "$BIN_ABS" "$INPUT_ABS")" || exit 2
read base_time base_energy <<< "$base_out"
echo "   Baseline:   ${base_time}s | ${base_energy}J"
echo "   Logs: $OUTDIR/baseline/baseline.log"

reset_governors_perf_list "$WORKER_CORES"
echo ""
echo "2) CONTROLLED (controller running on monitor core)"
ctrl_out="$(run_case "CONTROLLED" 1 "$MONITOR_CORE" "$WORKER_CORES" "$RANK0_CORE" "$BIN_ABS" "$INPUT_ABS")" || exit 3
read ctrl_time ctrl_energy <<< "$ctrl_out"
echo "   Controlled: ${ctrl_time}s | ${ctrl_energy}J"
echo "   Logs: $OUTDIR/controlled/controlled.log"
echo "         $OUTDIR/controlled/controller.log"
echo "         $OUTDIR/controlled/controller.csv"

echo ""
time_pct=$(echo "scale=4; (($ctrl_time - $base_time) / $base_time) * 100" | bc -l)
saved_j=$(echo "scale=6; $base_energy - $ctrl_energy" | bc -l)
energy_pct=$(echo "scale=4; (($base_energy - $ctrl_energy) / $base_energy) * 100" | bc -l)

echo "================ RESULTS ================"
printf "Runtime Impact: %+.2f%%\n" "$time_pct"
printf "Energy Savings: %s J (%.2f%%)\n" "$saved_j" "$energy_pct"

echo ""
echo "Artifacts:"
echo "  Rankfile:       $RANKFILE"
echo "  Baseline dir:   $OUTDIR/baseline/"
echo "  Controlled dir: $OUTDIR/controlled/"
