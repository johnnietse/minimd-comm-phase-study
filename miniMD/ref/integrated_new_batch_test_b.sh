#!/bin/bash
#SBATCH --job-name=minimd_test_b
#SBATCH --output=test_b_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=30
#SBATCH --cpus-per-task=1
#SBATCH --time=10:00:00
#SBATCH --mem=32G
#SBATCH --exclusive
#SBATCH --cpu-freq=performance
#SBATCH --partition=gpu-rgrant
#SBATCH --constraint=lkb
#SBATCH --nodelist=frnt115
#SBATCH --mail-user=22yht@queensu.ca
#SBATCH --mail-type=END,FAIL

# ============================================================
# TEST B — COMMUNICATION PHASE ON, NO FREQUENCY CONTROLLER
#
# Runs miniMD with comm phase enabled (default).
# All cores stay at default frequency under "performance" governor.
# Measures the overhead of adding the communication phase.
#
# Comparison:
#   A vs B → overhead of comm phase
#   B vs C → energy savings from frequency control
#
# Runs: 25 iterations × 6 MPI counts (1, 2, 4, 8, 16, 30)
# ============================================================

set -euo pipefail

# ---- OpenMP / MPI Environment ----
export OMP_NUM_THREADS=1
export OMP_PROC_BIND=true
export OMP_PLACES=cores

# ---- Configuration ----
MPI_COUNTS="1 2 4 8 16 30"
NUM_RUNS=25
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
ENERGY_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_ENERGY_UJ=$(cat /sys/class/powercap/intel-rapl:0/max_energy_range_uj 2>/dev/null || echo "65532610987")
IDLE_MEASURE_TIME=10

# ---- Output ----
CSV="results_test_b_${SLURM_JOB_ID}.csv"
SUMMARY_LOG="summary_test_b_${SLURM_JOB_ID}.txt"

# ---- Trap: Cleanup on failure or cancellation ----
cleanup() {
    echo ""
    echo "[CLEANUP] Restoring governors to performance..."
    for c in $(seq 0 29); do
        echo "performance" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor 2>/dev/null || true
    done
    rm -f .running_* .energy_tmp_* phase_marker.txt 2>/dev/null || true
    echo "[CLEANUP] Done."
}
trap cleanup EXIT INT TERM

# ---- Banner ----
echo "============================================================"
echo "TEST B — COMM PHASE ON, NO CONTROLLER"
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $(hostname)"
echo "Date:      $(date)"
echo "MPI Counts: $MPI_COUNTS"
echo "Runs/config: $NUM_RUNS"
echo "============================================================"

# ---- CPU Info ----
echo ""
echo "CPU Information:"
echo "  Model: $(lscpu | grep 'Model name' | cut -d: -f2 | xargs)"
echo "  Total Cores: $(nproc)"
FREQ_MIN=$(cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq 2>/dev/null || echo "0")
FREQ_MAX=$(cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq 2>/dev/null || echo "0")
echo "  Freq Range: $((FREQ_MIN/1000)) - $((FREQ_MAX/1000)) MHz"
echo "  Governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo 'N/A')"
echo "  Current Freq: $(($(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo 0)/1000)) MHz"
echo "  RAPL max range: $MAX_ENERGY_UJ uJ ($((MAX_ENERGY_UJ / 1000000)) J)"
echo ""

# ---- Prerequisites ----
if [ ! -f "$BINARY" ]; then
    echo "ERROR: $BINARY not found!"; exit 1
fi
if [ ! -f "$INPUT" ]; then
    echo "ERROR: $INPUT not found!"; exit 1
fi

# ---- Checkpoint directory ----
if [ -n "${SLURM_TMPDIR:-}" ] && [ -d "$SLURM_TMPDIR" ]; then
    CKPT_DIR="$SLURM_TMPDIR/chk"
    mkdir -p "$CKPT_DIR"
    rm -rf chk 2>/dev/null || true
    ln -sf "$CKPT_DIR" chk
    echo "Checkpoint dir: $CKPT_DIR (lscratch)"
else
    CKPT_DIR="./chk"
    mkdir -p "$CKPT_DIR"
    echo "Checkpoint dir: $CKPT_DIR (local)"
fi

# ---- Helper Functions ----
read_energy_uj() {
    cat "$ENERGY_FILE" 2>/dev/null || echo "0"
}

calc_energy_j() {
    local before=$1 after=$2
    local diff=$(( after - before ))
    if [ $diff -lt 0 ]; then
        diff=$(( diff + MAX_ENERGY_UJ ))
    fi
    echo "scale=3; $diff / 1000000" | bc -l
}

reset_governors() {
    local max_core=${1:-29}
    for c in $(seq 0 $max_core); do
        echo "performance" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor 2>/dev/null || true
    done
}

sample_energy() {
    local RUN_ID=$1
    local ACCUMULATED_UJ=0
    local PREV=$(cat "$ENERGY_FILE")

    while [ -f ".running_$RUN_ID" ]; do
        CURRENT=$(cat "$ENERGY_FILE")
        DIFF=$((CURRENT - PREV))
        if [ $DIFF -lt 0 ]; then DIFF=$((DIFF + MAX_ENERGY_UJ)); fi
        ACCUMULATED_UJ=$((ACCUMULATED_UJ + DIFF))
        PREV=$CURRENT
        echo $ACCUMULATED_UJ > ".energy_tmp_$RUN_ID"
        sleep 10
    done
}

# ---- Idle Baseline Measurement ----
echo "Measuring ${IDLE_MEASURE_TIME}s idle power baseline..."
reset_governors 29
sleep 2
IDLE_BEFORE=$(read_energy_uj)
sleep $IDLE_MEASURE_TIME
IDLE_AFTER=$(read_energy_uj)
IDLE_DIFF=$(( IDLE_AFTER - IDLE_BEFORE ))
if [ $IDLE_DIFF -lt 0 ]; then IDLE_DIFF=$(( IDLE_DIFF + MAX_ENERGY_UJ )); fi
IDLE_POWER_W=$(echo "scale=3; ($IDLE_DIFF / 1000000) / $IDLE_MEASURE_TIME" | bc -l)
echo "  Idle power: ${IDLE_POWER_W} W"
echo ""

# ---- CSV Header ----
echo "run,nprocs,energy_j,dynamic_energy_j,avg_power_w,idle_power_w,t_total,t_force,t_neigh,t_comm,t_other,performance,io_dur_s,io_bw_mbps,comm_dur_s,comm_bw_mbps,comm_data_mb" > "$CSV"

# ---- Main Experiment Loop ----
TOTAL_START=$(date +%s)

for N in $MPI_COUNTS; do
    echo ""
    echo "============================================================"
    echo "  MPI Processes: $N — Starting $NUM_RUNS runs"
    echo "============================================================"

    reset_governors 29
    ACTUAL_GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)
    echo "  Governor verified: $ACTUAL_GOV"

    for run in $(seq 1 $NUM_RUNS); do
        echo ""
        echo "--- N=$N, Run $run/$NUM_RUNS ---"

        rm -rf "$CKPT_DIR"/* 2>/dev/null || true
        mkdir -p "$CKPT_DIR"
        rm -f phase_marker.txt

        reset_governors $((N - 1))
        sleep 1

        TMPLOG=$(mktemp /tmp/minimd_b_${N}_${run}_XXXXXX.log)


		# Read energy BEFORE
        BEFORE_UJ=$(read_energy_uj)
        RUN_ID="b_${N}_${run}"
        touch ".running_$RUN_ID"
        sample_energy "$RUN_ID" &
        SAMPLER_PID=$!

        START_NS=$(date +%s%N)

        # Run miniMD — comm phase ENABLED (default)
        numactl --localalloc \
            mpirun -np $N --map-by core --bind-to core \
            "$BINARY" -i "$INPUT" \
            2>&1 | tee "$TMPLOG"

        EXIT_CODE=$?

        # Read energy AFTER
        AFTER_UJ=$(read_energy_uj)
        END_NS=$(date +%s%N)

        rm -f ".running_$RUN_ID"
        wait $SAMPLER_PID 2>/dev/null
        TOTAL_UJ=$(cat ".energy_tmp_$RUN_ID" 2>/dev/null || echo "0")
        rm -f ".energy_tmp_$RUN_ID"

        if [ $EXIT_CODE -ne 0 ]; then
            echo "WARNING: miniMD exited with code $EXIT_CODE (N=$N, run=$run)"
            rm -f "$TMPLOG"
            continue
        fi

        # Parse PERF_SUMMARY
        PERF_LINE=$(grep "PERF_SUMMARY" "$TMPLOG" | head -1)
        T_TOTAL=$(echo "$PERF_LINE" | awk '{print $5}')
        T_FORCE=$(echo "$PERF_LINE" | awk '{print $6}')
        T_NEIGH=$(echo "$PERF_LINE" | awk '{print $7}')
        T_COMM=$(echo "$PERF_LINE" | awk '{print $8}')
        T_OTHER=$(echo "$PERF_LINE" | awk '{print $9}')
        PERFORMANCE=$(echo "$PERF_LINE" | awk '{print $10}')

        IO_DUR=$(grep "Actual I/O duration:" "$TMPLOG" | awk '{print $4}' | head -1)
        IO_BW=$(grep "Average bandwidth:" "$TMPLOG" | awk '{print $3}' | head -1)

        COMM_DUR=$(grep "^Duration:" "$TMPLOG" | awk '{print $2}' | head -1)
        COMM_BW=$(grep "Effective bandwidth:" "$TMPLOG" | awk '{print $3}' | head -1)
        COMM_DATA=$(grep "Using runtime total:" "$TMPLOG" | awk '{print $4}' | head -1)

        if [ -z "$COMM_DUR" ]; then
            COMM_DUR=$(grep "Duration:" "$TMPLOG" | grep -v "I/O\|checkpoint\|Target" | awk '{print $2}' | head -1)
        fi

        # Calculate energy
        ENERGY_J=$(calc_energy_j "$BEFORE_UJ" "$AFTER_UJ")
        # if [ "$(echo "$T_TOTAL > 0" | bc -l 2>/dev/null)" = "1" ]; then
        #    AVG_POWER=$(echo "scale=2; $ENERGY_J / $T_TOTAL" | bc -l)

        # ENERGY_J=$(echo "scale=3; $TOTAL_UJ / 1000000" | bc -l)
        ELAPSED_NS=$(( END_NS - START_NS ))
        WALL_TIME=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc -l)

        if [ "$(echo "$WALL_TIME > 0" | bc -l 2>/dev/null)" = "1" ]; then
            AVG_POWER=$(echo "scale=2; $ENERGY_J / $WALL_TIME" | bc -l)
            DYNAMIC_J=$(echo "scale=3; $ENERGY_J - ($IDLE_POWER_W * $WALL_TIME)" | bc -l)
        else
            AVG_POWER="0"
            DYNAMIC_J="0"
        fi

        IO_DUR=${IO_DUR:-"N/A"}
        IO_BW=${IO_BW:-"N/A"}
        COMM_DUR=${COMM_DUR:-"N/A"}
        COMM_BW=${COMM_BW:-"N/A"}
        COMM_DATA=${COMM_DATA:-"N/A"}

        echo "$run,$N,$ENERGY_J,$DYNAMIC_J,$AVG_POWER,$IDLE_POWER_W,$T_TOTAL,$T_FORCE,$T_NEIGH,$T_COMM,$T_OTHER,$PERFORMANCE,$IO_DUR,$IO_BW,$COMM_DUR,$COMM_BW,$COMM_DATA" >> "$CSV"

        echo "  Energy: ${ENERGY_J} J (dynamic: ${DYNAMIC_J} J) | Power: ${AVG_POWER} W | Time: ${T_TOTAL} s | Comm: ${COMM_DUR} s"

        rm -f "$TMPLOG"
        sleep 2
    done
done

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$(( TOTAL_END - TOTAL_START ))

# ---- Summary ----
echo ""
echo "============================================================"
echo "TEST B COMPLETE"
echo "Total wall time: $(( TOTAL_ELAPSED / 3600 ))h $(( (TOTAL_ELAPSED % 3600) / 60 ))m $(( TOTAL_ELAPSED % 60 ))s"
echo "Results: $CSV"
echo "============================================================"

{
    echo "============================================================"
    echo "TEST B — SUMMARY (Comm phase ON, no controller)"
    echo "Job: $SLURM_JOB_ID | Node: $(hostname) | Date: $(date)"
    echo "Idle Power: ${IDLE_POWER_W} W"
    echo "============================================================"
    echo ""
    for N in $MPI_COUNTS; do
        echo "--- $N MPI processes ---"
        awk -F',' -v n="$N" '
            NR>1 && $2==n {
                count++; e+=$3; de+=$4; p+=$5; t+=$7; f+=$8
            }
            END {
                if(count>0) {
                    printf "  Runs: %d\n", count
                    printf "  Avg Energy:    %.1f J\n", e/count
                    printf "  Avg Dynamic E: %.1f J\n", de/count
                    printf "  Avg Power:     %.1f W\n", p/count
                    printf "  Avg Time:      %.2f s\n", t/count
                    printf "  Avg t_force:   %.2f s\n", f/count
                }
            }' "$CSV"
        echo ""
    done
} | tee "$SUMMARY_LOG"

# ---- Results Tarball ----
TARBALL="test_b_results_${SLURM_JOB_ID}.tar.gz"
echo "Creating results tarball: $TARBALL"
tar -czf "$TARBALL" "$CSV" "$SUMMARY_LOG" "test_b_${SLURM_JOB_ID}.out" 2>/dev/null || true
if [ -f "$TARBALL" ]; then
    cp "$TARBALL" ~/ 2>/dev/null || true
    echo "  Saved to: ~/$TARBALL"
    echo ""
    echo "To download:"
    echo "  scp hpc6081@login1:~/$TARBALL ."
fi

reset_governors 29
echo ""
echo "Governors restored. Done."
