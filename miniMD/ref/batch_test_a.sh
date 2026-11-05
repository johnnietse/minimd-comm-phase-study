#!/bin/bash
#SBATCH --job-name=minimd_test_a
#SBATCH --output=test_a_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=30
#SBATCH --cpus-per-task=1
#SBATCH --time=10:00:00
#SBATCH --mem=32G
#SBATCH --exclusive
#SBATCH --partition=gpu-rgrant
#SBATCH --constraint=lkb

# ============================================================
# TEST A — BASELINE (no communication phase, performance governor)
#
# Runs miniMD with --comm_phase 0 (comm phase disabled).
# All cores stay at default frequency under "performance" governor.
# This is the control run to compare against Tests B and C.
#
# Runs: 25 iterations × 6 MPI counts (1, 2, 4, 8, 16, 30)
# ============================================================

set -euo pipefail
export OMP_NUM_THREADS=1

# ---- Configuration ----
MPI_COUNTS="1 2 4 8 16 30"
NUM_RUNS=25
BINARY="./miniMD_openmpi"
INPUT="in.lj.miniMD"
ENERGY_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_ENERGY_UJ=$(cat /sys/class/powercap/intel-rapl:0/max_energy_range_uj 2>/dev/null || echo "65532610987")

# ---- Output ----
CSV="results_test_a_${SLURM_JOB_ID}.csv"
SUMMARY_LOG="summary_test_a_${SLURM_JOB_ID}.txt"

echo "============================================================"
echo "TEST A — BASELINE (comm_phase OFF)"
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $(hostname)"
echo "Date:      $(date)"
echo "MPI Counts: $MPI_COUNTS"
echo "Runs/config: $NUM_RUNS"
echo "============================================================"

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

# ---- CSV Header ----
echo "run,nprocs,energy_j,avg_power_w,t_total,t_force,t_neigh,t_comm,t_other,performance,io_dur_s,io_bw_mbps" > "$CSV"

# ---- Main Experiment Loop ----
TOTAL_START=$(date +%s)

for N in $MPI_COUNTS; do
    echo ""
    echo "============================================================"
    echo "  MPI Processes: $N — Starting $NUM_RUNS runs"
    echo "============================================================"

    # Ensure performance governor on all cores we'll use
    reset_governors 29

    for run in $(seq 1 $NUM_RUNS); do
        echo ""
        echo "--- N=$N, Run $run/$NUM_RUNS ---"

        # Clean checkpoint directory
        rm -rf "$CKPT_DIR"/* 2>/dev/null || true
        mkdir -p "$CKPT_DIR"

        # Ensure clean governor state
        reset_governors $((N - 1))
        sleep 1

        # Temporary log file
        TMPLOG=$(mktemp /tmp/minimd_a_${N}_${run}_XXXXXX.log)

        # Read energy BEFORE
        BEFORE_UJ=$(read_energy_uj)

        # Run miniMD — comm phase DISABLED
        mpirun -np $N --map-by core --bind-to core \
            "$BINARY" -i "$INPUT" --comm_phase 0 \
            2>&1 | tee "$TMPLOG"

        EXIT_CODE=$?

        # Read energy AFTER
        AFTER_UJ=$(read_energy_uj)

        if [ $EXIT_CODE -ne 0 ]; then
            echo "WARNING: miniMD exited with code $EXIT_CODE (N=$N, run=$run)"
            rm -f "$TMPLOG"
            continue
        fi

        # Parse PERF_SUMMARY line
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

        # Calculate energy
        ENERGY_J=$(calc_energy_j "$BEFORE_UJ" "$AFTER_UJ")
        if [ "$(echo "$T_TOTAL > 0" | bc -l 2>/dev/null)" = "1" ]; then
            AVG_POWER=$(echo "scale=2; $ENERGY_J / $T_TOTAL" | bc -l)
        else
            AVG_POWER="0"
        fi

        # Default missing values
        IO_DUR=${IO_DUR:-"N/A"}
        IO_BW=${IO_BW:-"N/A"}

        # Write CSV row
        echo "$run,$N,$ENERGY_J,$AVG_POWER,$T_TOTAL,$T_FORCE,$T_NEIGH,$T_COMM,$T_OTHER,$PERFORMANCE,$IO_DUR,$IO_BW" >> "$CSV"

        echo "  Energy: ${ENERGY_J} J | Power: ${AVG_POWER} W | Time: ${T_TOTAL} s | Perf: ${PERFORMANCE}"

        # Cleanup
        rm -f "$TMPLOG"

        sleep 2
    done
done

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$(( TOTAL_END - TOTAL_START ))

# ---- Summary ----
echo ""
echo "============================================================"
echo "TEST A COMPLETE"
echo "Total wall time: $(( TOTAL_ELAPSED / 3600 ))h $(( (TOTAL_ELAPSED % 3600) / 60 ))m $(( TOTAL_ELAPSED % 60 ))s"
echo "Results: $CSV"
echo "============================================================"

# Per-config averages
{
    echo "============================================================"
    echo "TEST A — SUMMARY (Baseline, comm_phase OFF)"
    echo "Job: $SLURM_JOB_ID | Node: $(hostname) | Date: $(date)"
    echo "============================================================"
    echo ""
    for N in $MPI_COUNTS; do
        echo "--- $N MPI processes ---"
        awk -F',' -v n="$N" '
            NR>1 && $2==n {
                count++; e+=$3; p+=$4; t+=$5; f+=$6
            }
            END {
                if(count>0) {
                    printf "  Runs: %d\n", count
                    printf "  Avg Energy:  %.1f J\n", e/count
                    printf "  Avg Power:   %.1f W\n", p/count
                    printf "  Avg Time:    %.2f s\n", t/count
                    printf "  Avg t_force: %.2f s\n", f/count
                }
            }' "$CSV"
        echo ""
    done
} | tee "$SUMMARY_LOG"

# Restore governors
reset_governors 29
echo "Governors restored to performance."
echo "Done."
