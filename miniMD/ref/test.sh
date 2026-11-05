#!/bin/bash
set -euo pipefail

# ================= CONFIG =================
N=${1:-26}
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
CONTROLLER="./monitor.py"

RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

WORKER_CORE_RANGE="0-29"
MONITOR_CORE="30"
RESERVED_CORE="31"

FREQ_LOW=1200000
FREQ_MID=1600000
POLL_MS=2
LOW_AFTER_MS=2
MID_AFTER_MS=5

# RAPL sampling interval for wrap-safe accumulation
ENERGY_SAMPLE_INTERVAL=0.5

OUTDIR="run_$(date +%Y%m%d_%H%M%S)_n${N}"
mkdir -p "$OUTDIR"

HINT_FILE="/dev/shm/minimd_phase_hints_${USER}_$$.bin"

CTRL_LOG="$OUTDIR/controller.log"
BASE_LOG="$OUTDIR/baseline.log"
CTRL_RUN_LOG="$OUTDIR/controlled.log"
SUMMARY="$OUTDIR/summary.txt"

MON_PID=""
SAMPLE_PID=""

cleanup() {
  if [[ -n "${SAMPLE_PID}" ]] && kill -0 "${SAMPLE_PID}" 2>/dev/null; then
    kill -TERM "${SAMPLE_PID}" 2>/dev/null || true
    wait "${SAMPLE_PID}" 2>/dev/null || true
    SAMPLE_PID=""
  fi

  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "${MON_PID}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${MON_PID}" 2>/dev/null; then
      kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    fi
    wait "${MON_PID}" 2>/dev/null || true
    MON_PID=""
  fi

  rm -f "$HINT_FILE" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

read_energy_uj() { cat "$RAPL_FILE" 2>/dev/null || echo "0"; }
read_max_uj() { cat "$MAX_RAPL_FILE" 2>/dev/null || echo "262143328850"; }

energy_diff_uj() {
  local before=$1 after=$2 maxv=$3
  local diff=$(( after - before ))
  if (( diff < 0 )); then
    diff=$(( diff + maxv ))
  fi
  echo "$diff"
}

now_ns() { date +%s%N; }

reset_governors_perf_range() {
  for c in $(seq 4 29); do
    echo "performance" > "/sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor" 2>/dev/null || true
  done
}

start_controller() {
  : > "$CTRL_LOG"
  rm -f "$HINT_FILE"

  PHASE_HINT_PATH="$HINT_FILE" \
  taskset -c "$MONITOR_CORE" python3 -u "$CONTROLLER" \
    --hint-file "$HINT_FILE" \
    --freq-low "$FREQ_LOW" \
    --freq-mid "$FREQ_MID" \
    --poll-ms "$POLL_MS" \
    --low-after-ms "$LOW_AFTER_MS" \
    --mid-after-ms "$MID_AFTER_MS" \
    > "$CTRL_LOG" 2>&1 &

  MON_PID=$!
  sleep 0.3
}

stop_controller_hard() {
  if [[ -n "${MON_PID}" ]] && kill -0 "${MON_PID}" 2>/dev/null; then
    kill -SIGINT "${MON_PID}" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "${MON_PID}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${MON_PID}" 2>/dev/null; then
      kill -SIGKILL "${MON_PID}" 2>/dev/null || true
    fi
    wait "${MON_PID}" 2>/dev/null || true
  fi
  MON_PID=""
}

# Periodically sample RAPL and accumulate energy in microjoules.
# This is robust to wraparound as long as at most one wrap occurs per sample interval.
sample_energy_accumulate() {
  local watched_pid="$1"
  local outfile="$2"

  local maxv prev cur delta total
  maxv="$(read_max_uj)"
  prev="$(read_energy_uj)"
  total=0

  while kill -0 "$watched_pid" 2>/dev/null; do
    sleep "$ENERGY_SAMPLE_INTERVAL"
    cur="$(read_energy_uj)"
    delta=$(( cur - prev ))
    if (( delta < 0 )); then
      delta=$(( delta + maxv ))
    fi
    total=$(( total + delta ))
    prev=$cur
  done

  # one final read right after process exit
  cur="$(read_energy_uj)"
  delta=$(( cur - prev ))
  if (( delta < 0 )); then
    delta=$(( delta + maxv ))
  fi
  total=$(( total + delta ))

  echo "$total" > "$outfile"
}

run_case() {
  local label="$1"
  local log="$2"
  local use_controller="$3"

  local energy_tmp="$OUTDIR/${label,,}_energy_uj.txt"

  rm -f "$HINT_FILE" "$energy_tmp"

  if [[ "$use_controller" == "1" ]]; then
    start_controller
  else
    MON_PID=""
  fi

  local start_ns end_ns
  start_ns="$(now_ns)"

  (
    OMP_NUM_THREADS=1 \
    OMP_PROC_BIND=true \
    OMP_PLACES=cores \
    PHASE_HINT_PATH="$HINT_FILE" \
    taskset -c "$WORKER_CORE_RANGE" \
    mpirun -np "$N" \
      --bind-to core \
      --map-by core \
      --report-bindings \
      "$BINARY" -i "$INPUT" > "$log" 2>&1
  ) &
  local mpirun_pid=$!

  sample_energy_accumulate "$mpirun_pid" "$energy_tmp" &
  SAMPLE_PID=$!

  set +e
  wait "$mpirun_pid"
  local mpistatus=$?
  set -e

  if [[ -n "${SAMPLE_PID}" ]]; then
    wait "${SAMPLE_PID}" 2>/dev/null || true
    SAMPLE_PID=""
  fi

  end_ns="$(now_ns)"

  stop_controller_hard

  if [[ $mpistatus -ne 0 ]]; then
    echo "ERROR: mpirun exited with status $mpistatus. See log: $log" >&2
    return $mpistatus
  fi

  if [[ ! -f "$energy_tmp" ]]; then
    echo "ERROR: Energy sampler did not produce $energy_tmp" >&2
    return 1
  fi

  local diff_uj
  diff_uj="$(cat "$energy_tmp")"

  local wall_s energy_j avg_w
  wall_s="$(echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc -l)"
  energy_j="$(echo "scale=6; $diff_uj / 1000000" | bc -l)"
  avg_w="$(echo "scale=6; $energy_j / $wall_s" | bc -l)"

  echo "$wall_s $energy_j $avg_w"
}

echo "=========================================================="
echo "miniMD baseline vs phase-aware monitor"
echo "MPI ranks / worker cores: $N"
echo "Worker cores: $WORKER_CORE_RANGE"
echo "Monitor core: $MONITOR_CORE"
echo "Reserved core: $RESERVED_CORE"
echo "Output dir: $OUTDIR"
echo "Hint file: $HINT_FILE"
echo "RAPL sample interval: ${ENERGY_SAMPLE_INTERVAL}s"
echo "=========================================================="

[[ -f "$BINARY" ]] || { echo "ERROR: $BINARY not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: $INPUT not found"; exit 1; }
[[ -f "$CONTROLLER" ]] || { echo "ERROR: $CONTROLLER not found"; exit 1; }

reset_governors_perf_range

echo ""
echo "1) BASELINE (no monitor)"
if read base_time base_energy base_power <<< "$(run_case "BASELINE" "$BASE_LOG" 0)"; then
  echo "   Baseline:   ${base_time}s | ${base_energy}J | avg ${base_power}W"
else
  echo "Baseline failed. Last 80 lines:"
  tail -n 80 "$BASE_LOG" || true
  exit 1
fi

reset_governors_perf_range

echo ""
echo "2) CONTROLLED (monitor on dedicated core)"
if read ctrl_time ctrl_energy ctrl_power <<< "$(run_case "CONTROLLED" "$CTRL_RUN_LOG" 1)"; then
  echo "   Controlled: ${ctrl_time}s | ${ctrl_energy}J | avg ${ctrl_power}W"
else
  echo "Controlled failed. Last 80 lines:"
  tail -n 80 "$CTRL_RUN_LOG" || true
  exit 1
fi

reset_governors_perf_range || true

time_pct=$(echo "scale=4; (($ctrl_time - $base_time) / $base_time) * 100" | bc -l)
saved_j=$(echo "scale=6; $base_energy - $ctrl_energy" | bc -l)
energy_pct=$(echo "scale=4; (($base_energy - $ctrl_energy) / $base_energy) * 100" | bc -l)

{
  echo "================ RESULTS ================"
  printf "Runtime Impact: %+.2f%%\n" "$time_pct"
  printf "Energy Savings: %s J (%.2f%%)\n" "$saved_j" "$energy_pct"
  printf "Baseline Avg Power:   %s W\n" "$base_power"
  printf "Controlled Avg Power: %s W\n" "$ctrl_power"
  echo "Logs:"
  echo "  Baseline:    $BASE_LOG"
  echo "  Controlled:  $CTRL_RUN_LOG"
  echo "  Controller:  $CTRL_LOG"
  echo "  Summary:     $SUMMARY"
} | tee "$SUMMARY"
