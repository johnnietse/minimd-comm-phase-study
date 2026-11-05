#!/bin/bash
set -euo pipefail

# ================= CONFIG =================
N=${1:-8}  # must be 1,2,4,8,16,30 for your controller assumptions
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
CONTROLLER="./comm_freq_controller.py"

RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

OUTDIR="run_$(date +%Y%m%d_%H%M%S)_n${N}"
mkdir -p "$OUTDIR"

CTRL_LOG="$OUTDIR/controller.log"
BASE_LOG="$OUTDIR/baseline.log"
CTRL_RUN_LOG="$OUTDIR/controlled.log"
RANKFILE="$OUTDIR/host_rankfile"

MON_PID=""

cleanup() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    wait "${MON_PID}" 2>/dev/null || true
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

# Parse cpuset cores for THIS shell process, return string like "0-3,6,8-9"
get_cpuset_str() {
  taskset -cp $$ | awk -F': ' '{print $2}'
}

# Convert "0-2,5,7-9" -> "0 1 2 5 7 8 9"
expand_core_list() {
  python3 - <<'PY'
import sys
s=sys.stdin.read().strip()
cores=set()
if s:
  for part in s.split(','):
    part=part.strip()
    if not part:
      continue
    if '-' in part:
      a,b=part.split('-')
      for c in range(int(a), int(b)+1):
        cores.add(c)
    else:
      cores.add(int(part))
print(" ".join(map(str, sorted(cores))))
PY
}

# Pick worker cores AND monitor core from cpuset.
# REQUIREMENT (because your controller assumes it): workers are exactly 0..N-1.
pick_layout() {
  local n="$1"
  local cpuset_str expanded
  cpuset_str="$(get_cpuset_str)"
  expanded="$(echo "$cpuset_str" | expand_core_list)"

  python3 - <<PY
import sys
n=int("$n")
avail=list(map(int, "$expanded".split())) if "$expanded".strip() else []
S=set(avail)

# Must have exact worker cores 0..n-1 due to controller logic
need=list(range(n))
missing=[c for c in need if c not in S]
if missing:
  sys.stderr.write("ERROR: Allocation does not include required worker cores 0..%d\\n" % (n-1))
  sys.stderr.write("  Missing cores: %s\\n" % missing)
  sys.stderr.write("  Your controller hard-codes workers as cores 0..N-1, so it would control the wrong CPUs.\\n")
  sys.stderr.write("  Fix: request an allocation that includes cores 0..N-1 (or modify controller to accept an explicit core list).\\n")
  sys.exit(2)

workers=need

# Pick monitor as highest available core not used by workers
candidates=[c for c in avail if c not in set(workers)]
if not candidates:
  sys.stderr.write("ERROR: No spare core for monitor. Need N workers + 1 extra core in your allocation.\\n")
  sys.exit(3)

mon=max(candidates)

print(",".join(map(str,workers)) + "|" + str(mon))
PY
}

make_rankfile() {
  # rank i -> core i (0..N-1). Matches controller assumption.
  local n="$1"
  : > "$RANKFILE"
  for ((r=0; r<n; r++)); do
    echo "rank $r=localhost slot=$r" >> "$RANKFILE"
  done
}

reset_governors_perf_workers() {
  # Only reset worker cores 0..N-1 (don’t touch other allocated cores)
  local n="$1"
  for ((c=0; c<n; c++)); do
    echo "performance" > "/sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor" 2>/dev/null || true
  done
}

run_case() {
  local label="$1"          # BASELINE or CONTROLLED
  local log="$2"            # output log file
  local use_controller="$3" # 0/1
  local monitor_core="$4"

  # Start controller if needed (no waiting for readiness)
  if [[ "$use_controller" == "1" ]]; then
    : > "$CTRL_LOG"
    taskset -c "$monitor_core" python3 -u "$CONTROLLER" --workers "$N" > "$CTRL_LOG" 2>&1 &
    MON_PID=$!
    # small settle so governor/freq writes happen before measurement starts
    sleep 0.2
  else
    MON_PID=""
  fi

  local max_uj before_uj after_uj diff_uj
  local start_ns end_ns
  max_uj="$(read_max_uj)"
  before_uj="$(read_energy_uj)"
  start_ns="$(now_ns)"

  mpirun -np "$N" \
    --rankfile "$RANKFILE" \
    --report-bindings \
    "$BINARY" -i "$INPUT" > "$log" 2>&1

  end_ns="$(now_ns)"
  after_uj="$(read_energy_uj)"

  # Stop controller after run
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    wait "${MON_PID}" 2>/dev/null || true
    MON_PID=""
  fi

  diff_uj="$(energy_diff_uj "$before_uj" "$after_uj" "$max_uj")"

  local wall_s energy_j
  wall_s="$(echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc -l)"
  energy_j="$(echo "scale=6; $diff_uj / 1000000" | bc -l)"

  echo "$wall_s $energy_j"
}

# ================= MAIN =================
echo "=========================================================="
echo "miniMD + comm_freq_controller evaluation"
echo "Workers (MPI ranks): $N"
echo "Output dir: $OUTDIR"
echo "=========================================================="

# Basic checks
[[ -f "$BINARY" ]] || { echo "ERROR: $BINARY not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: $INPUT not found"; exit 1; }
[[ -f "$CONTROLLER" ]] || { echo "ERROR: $CONTROLLER not found"; exit 1; }

# Show cpuset and expanded list
CPUSET_STR="$(get_cpuset_str)"
CPUSET_EXPANDED="$(echo "$CPUSET_STR" | expand_core_list)"
echo "[INFO] taskset cpuset: $CPUSET_STR"
echo "[INFO] expanded cores: $CPUSET_EXPANDED"

# Pick worker cores + monitor core automatically
LAYOUT="$(pick_layout "$N")"
WORKER_CORES="$(echo "$LAYOUT" | cut -d'|' -f1)"
MONITOR_CORE="$(echo "$LAYOUT" | cut -d'|' -f2)"

echo "[INFO] Worker cores: $WORKER_CORES (must be 0..$((N-1)) for this controller)"
echo "[INFO] Monitor core: $MONITOR_CORE (auto-picked)"

# Build rankfile (rank i -> core i)
make_rankfile "$N"

# Reset worker governors back to performance before baseline
reset_governors_perf_workers "$N"

echo ""
echo "1) BASELINE (no controller)"
read base_time base_energy <<< "$(run_case "BASELINE" "$BASE_LOG" 0 "$MONITOR_CORE")"
echo "   Baseline:   ${base_time}s | ${base_energy}J"

# Reset worker governors before controlled run
reset_governors_perf_workers "$N"

echo ""
echo "2) CONTROLLED (controller running on monitor core)"
read ctrl_time ctrl_energy <<< "$(run_case "CONTROLLED" "$CTRL_RUN_LOG" 1 "$MONITOR_CORE")"
echo "   Controlled: ${ctrl_time}s | ${ctrl_energy}J"

# Results
echo ""
if (( $(echo "$base_time > 0" | bc -l) )); then
  time_pct=$(echo "scale=4; (($ctrl_time - $base_time) / $base_time) * 100" | bc -l)
  saved_j=$(echo "scale=6; $base_energy - $ctrl_energy" | bc -l)
  energy_pct=$(echo "scale=4; (($base_energy - $ctrl_energy) / $base_energy) * 100" | bc -l)

  echo "================ RESULTS ================"
  printf "Runtime Impact: %+.2f%%\n" "$time_pct"
  printf "Energy Savings: %s J (%.2f%%)\n" "$saved_j" "$energy_pct"
  echo "Logs:"
  echo "  Baseline:    $BASE_LOG"
  echo "  Controlled:  $CTRL_RUN_LOG"
  echo "  Controller:  $CTRL_LOG"
else
  echo "ERROR: Baseline time was 0"
fi
