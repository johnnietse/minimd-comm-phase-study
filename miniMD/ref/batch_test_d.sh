#!/bin/bash
#SBATCH --job-name=minimd_test_d
#SBATCH --output=test_d_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=30
#SBATCH --cpus-per-task=1
#SBATCH --time=10:00:00
#SBATCH --mem=32G
#SBATCH --exclusive
#SBATCH --partition=gpu-rgrant
#SBATCH --constraint=lkb
#SBATCH --nodelist=frnt115
#SBATCH --mail-user=22yht@queensu.ca
#SBATCH --mail-type=END,FAIL

# ============================================================
# TEST D — FIXED FREQUENCY SWEEP
#
# Runs miniMD at fixed CPU frequencies to show the tradeoff
# between energy savings and performance slowdown.
# This proves why Test C's dynamic approach is superior:
#   - Low fixed freq  → saves energy but HURTS performance
#   - High fixed freq → good performance but WASTES energy
#   - Test C (dynamic) → saves energy WITHOUT hurting performance
#
# Frequencies: 1200 MHz, 1600 MHz, 2000 MHz, 2400 MHz
# Comm phase: ON (same as Test B, for fair comparison)
#
# Runs: 25 iterations × 6 MPI counts × 4 frequencies
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
IDLE_MEASURE_TIME=10  # seconds — idle baseline measurement

# Fixed frequencies to sweep (in kHz, as expected by cpufreq sysfs)
FREQ_LIST="1200000 1600000 2000000 2400000"

# ---- Output ----
CSV="results_test_d_${SLURM_JOB_ID}.csv"
SUMMARY_LOG="summary_test_d_${SLURM_JOB_ID}.txt"

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
echo "TEST D — FIXED FREQUENCY SWEEP"
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $(hostname)"
echo "Date:      $(date)"
echo "Frequencies: $FREQ_LIST"
echo "MPI Counts: $MPI_COUNTS"
echo "Runs/config: $NUM_RUNS"
echo "============================================================"

# ---- CPU Info ----
echo ""
echo "CPU Information:"
echo "  Model: $(lscpu | grep 'Model name' | cut -d: -f2 | xargs)"
echo "  Total Cores: $(nproc)"
FREQ_MIN_HW=$(cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq 2>/dev/null || echo "0")
FREQ_MAX_HW=$(cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq 2>/dev/null || echo "0")
echo "  HW Freq Range: $((FREQ_MIN_HW/1000)) - $((FREQ_MAX_HW/1000)) MHz"
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

set_fixed_freq() {
    local freq=$1
    local max_core=${2:-29}
    for c in $(seq 0 $max_core); do
        echo "userspace" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor 2>/dev/null || true
        echo "$freq" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_setspeed 2>/dev/null || true
    done
}

verify_freq() {
    local expected=$1
    local actual=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo "0")
    local gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "N/A")
    echo "  Governor: $gov | Target: $((expected/1000)) MHz | Actual: $((actual/1000)) MHz"
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
echo "run,nprocs,target_freq_mhz,actual_freq_mhz,energy_j,dynamic_energy_j,avg_power_w,idle_power_w,t_total,t_force,t_neigh,t_comm,t_other,performance,io_dur_s,io_bw_mbps,comm_dur_s,comm_bw_mbps,comm_data_mb" > "$CSV"

# ---- Main Experiment Loop ----
TOTAL_START=$(date +%s)

for FREQ in $FREQ_LIST; do
    FREQ_MHZ=$((FREQ / 1000))
    echo ""
    echo "============================================================"
    echo "  FREQUENCY: ${FREQ_MHZ} MHz"
    echo "============================================================"

    for N in $MPI_COUNTS; do
        echo ""
        echo "  ---- MPI Processes: $N — Starting $NUM_RUNS runs @ ${FREQ_MHZ} MHz ----"

        for run in $(seq 1 $NUM_RUNS); do
            echo ""
            echo "--- Freq=${FREQ_MHZ}MHz, N=$N, Run $run/$NUM_RUNS ---"

            # Clean checkpoint directory
            rm -rf "$CKPT_DIR"/* 2>/dev/null || true
            mkdir -p "$CKPT_DIR"

            # Clean phase marker
            rm -f phase_marker.txt

            # Set fixed frequency on all worker cores
            set_fixed_freq $FREQ 29
            sleep 1

            # Verify frequency was applied
            verify_freq $FREQ

            TMPLOG=$(mktemp /tmp/minimd_d_${FREQ_MHZ}_${N}_${run}_XXXXXX.log)

            # Read energy BEFORE
            BEFORE_UJ=$(read_energy_uj)
            START_NS=$(date +%s%N)

            # Run miniMD — comm phase ENABLED (same as Test B)
            mpirun -np $N --map-by core --bind-to core \
                "$BINARY" -i "$INPUT" \
                2>&1 | tee "$TMPLOG"

            EXIT_CODE=$?

            # Read energy AFTER
            AFTER_UJ=$(read_energy_uj)
            END_NS=$(date +%s%N)

            # Read actual frequency during/after run
            ACTUAL_FREQ=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo "0")
            ACTUAL_MHZ=$((ACTUAL_FREQ / 1000))

            if [ $EXIT_CODE -ne 0 ]; then
                echo "WARNING: miniMD exited with code $EXIT_CODE (Freq=${FREQ_MHZ}, N=$N, run=$run)"
                rm -f "$TMPLOG"
                # Reset to performance before continuing
                reset_governors 29
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

            # Parse I/O metrics
            IO_DUR=$(grep "Actual I/O duration:" "$TMPLOG" | awk '{print $4}' | head -1)
            IO_BW=$(grep "Average bandwidth:" "$TMPLOG" | awk '{print $3}' | head -1)

            # Parse Communication phase metrics
            COMM_DUR=$(grep "^Duration:" "$TMPLOG" | awk '{print $2}' | head -1)
            COMM_BW=$(grep "Effective bandwidth:" "$TMPLOG" | awk '{print $3}' | head -1)
            COMM_DATA=$(grep "Using runtime total:" "$TMPLOG" | awk '{print $4}' | head -1)

            if [ -z "$COMM_DUR" ]; then
                COMM_DUR=$(grep "Duration:" "$TMPLOG" | grep -v "I/O\|checkpoint\|Target" | awk '{print $2}' | head -1)
            fi

            # Calculate energy
            ENERGY_J=$(calc_energy_j "$BEFORE_UJ" "$AFTER_UJ")

            # Wall-clock time (nanosecond precision)
            ELAPSED_NS=$(( END_NS - START_NS ))
            WALL_TIME=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc -l)

            if [ "$(echo "$WALL_TIME > 0" | bc -l 2>/dev/null)" = "1" ]; then
                AVG_POWER=$(echo "scale=2; $ENERGY_J / $WALL_TIME" | bc -l)
                DYNAMIC_J=$(echo "scale=3; $ENERGY_J - ($IDLE_POWER_W * $WALL_TIME)" | bc -l)
            else
                AVG_POWER="0"
                DYNAMIC_J="0"
            fi

            # Defaults
            IO_DUR=${IO_DUR:-"N/A"}
            IO_BW=${IO_BW:-"N/A"}
            COMM_DUR=${COMM_DUR:-"N/A"}
            COMM_BW=${COMM_BW:-"N/A"}
            COMM_DATA=${COMM_DATA:-"N/A"}

            # Write CSV row
            echo "$run,$N,$FREQ_MHZ,$ACTUAL_MHZ,$ENERGY_J,$DYNAMIC_J,$AVG_POWER,$IDLE_POWER_W,$T_TOTAL,$T_FORCE,$T_NEIGH,$T_COMM,$T_OTHER,$PERFORMANCE,$IO_DUR,$IO_BW,$COMM_DUR,$COMM_BW,$COMM_DATA" >> "$CSV"

            echo "  Freq: ${FREQ_MHZ}/${ACTUAL_MHZ} MHz | Energy: ${ENERGY_J} J (dynamic: ${DYNAMIC_J} J) | Power: ${AVG_POWER} W | Time: ${T_TOTAL} s"

            rm -f "$TMPLOG"
            sleep 2
        done
    done
done

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$(( TOTAL_END - TOTAL_START ))

# ---- Summary ----
echo ""
echo "============================================================"
echo "TEST D COMPLETE"
echo "Total wall time: $(( TOTAL_ELAPSED / 3600 ))h $(( (TOTAL_ELAPSED % 3600) / 60 ))m $(( TOTAL_ELAPSED % 60 ))s"
echo "Results: $CSV"
echo "============================================================"

{
    echo "============================================================"
    echo "TEST D — SUMMARY (Fixed Frequency Sweep)"
    echo "Job: $SLURM_JOB_ID | Node: $(hostname) | Date: $(date)"
    echo "Idle Power: ${IDLE_POWER_W} W"
    echo "============================================================"
    echo ""
    for FREQ in $FREQ_LIST; do
        FREQ_MHZ=$((FREQ / 1000))
        echo "=== ${FREQ_MHZ} MHz ==="
        for N in $MPI_COUNTS; do
            echo "--- $N MPI processes @ ${FREQ_MHZ} MHz ---"
            awk -F',' -v n="$N" -v f="$FREQ_MHZ" '
                NR>1 && $2==n && $3==f {
                    count++; e+=$5; de+=$6; p+=$7; t+=$9; perf+=$14
                }
                END {
                    if(count>0) {
                        printf "  Runs: %d\n", count
                        printf "  Avg Energy:    %.1f J\n", e/count
                        printf "  Avg Dynamic E: %.1f J\n", de/count
                        printf "  Avg Power:     %.1f W\n", p/count
                        printf "  Avg Time:      %.2f s\n", t/count
                        printf "  Avg Perf:      %.2f\n", perf/count
                    }
                }' "$CSV"
            echo ""
        done
    done
} | tee "$SUMMARY_LOG"

# ---- Create Results Tarball ----
TARBALL="test_d_results_${SLURM_JOB_ID}.tar.gz"
echo "Creating results tarball: $TARBALL"
tar -czf "$TARBALL" "$CSV" "$SUMMARY_LOG" "test_d_${SLURM_JOB_ID}.out" 2>/dev/null || true
if [ -f "$TARBALL" ]; then
    cp "$TARBALL" ~/ 2>/dev/null || true
    echo "  Saved to: ~/$TARBALL"
    echo ""
    echo "To download:"
    echo "  scp hpc6081@login1:~/$TARBALL ."
fi

# Restore governors
reset_governors 29
echo ""
echo "Governors restored. Done."
