#!/bin/bash
#SBATCH --nodes=1
#SBATCH --partition=gpu-rgrant
#SBATCH --constraint=lkb
#SBATCH --nodelist=frnt115
#SBATCH --ntasks=32
#SBATCH --cpus-per-task=1
#SBATCH --exclusive
#SBATCH --time=9:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=22yht@queensu.ca


# ==============================================================================
# Automated Test B — COMM PHASE ON, NO CONTROLLER (performance governor)
# Loops over MPI ranks 1, 2, 4, 8, 16, 30 and runs 1 to 25.
# Saves individual combined files (log + csv line) for each specific run.
# ==============================================================================

set -euo pipefail

MPI_COUNTS="1 2 4 8 16 30"
NUM_RUNS=25

# Calculate total runs for progress tracking
NUM_MPI_COUNTS=$(echo $MPI_COUNTS | wc -w)
TOTAL_RUNS=$(( NUM_RUNS * NUM_MPI_COUNTS ))
CURRENT_RUN=0

# Create a master CSV header if it doesn't exist
CSV_FILE="results_manual_test_b.csv"
if [ ! -f "$CSV_FILE" ]; then
    echo "run,nprocs,energy_j,t_total,t_force,t_neigh,t_comm,t_other,performance" > "$CSV_FILE"
fi

for NUM_WORKERS in $MPI_COUNTS; do
    echo "============================================================"
    echo "Starting tests for MPI Rank: $NUM_WORKERS"
    echo "============================================================"

    for RUN in $(seq 1 $NUM_RUNS); do
        CURRENT_RUN=$(( CURRENT_RUN + 1 ))
        PROGRESS_PCT=$(( (CURRENT_RUN * 100) / TOTAL_RUNS ))
        
        echo ""
        echo "============================================================"
        echo "PROGRESS: [ $CURRENT_RUN / $TOTAL_RUNS ] ($PROGRESS_PCT% Complete)"
        echo "--- Workers: $NUM_WORKERS, Run: $RUN ---"
        echo "============================================================"

        # 3a. Reset governors
        for c in $(seq 0 $((NUM_WORKERS - 1))); do
            echo "performance" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor 2>/dev/null || true
        done

        # 3b. Clean phase marker
        rm -f phase_marker.txt

        # 3c. Read energy BEFORE
        MAX_RANGE=$(cat /sys/class/powercap/intel-rapl:0/max_energy_range_uj 2>/dev/null || echo "65532610987")
        BEFORE=$(cat /sys/class/powercap/intel-rapl:0/energy_uj 2>/dev/null || echo "0")

        LOG_FILE="/tmp/test_b_${NUM_WORKERS}_${RUN}.log"

        # 3d. Run miniMD — comm phase ENABLED (default, no --comm_phase flag)
        mpirun -np $NUM_WORKERS --bind-to core \
            ./miniMD_openmpi -i in.lj.miniMD \
            2>&1 | tee "$LOG_FILE"

        # 3e. Read energy AFTER (with wraparound)
        AFTER=$(cat /sys/class/powercap/intel-rapl:0/energy_uj 2>/dev/null || echo "0")
        DIFF=$(( AFTER - BEFORE ))
        if [ $DIFF -lt 0 ]; then DIFF=$(( DIFF + MAX_RANGE )); fi
        ENERGY_J=$(echo "scale=3; $DIFF / 1000000" | bc -l)
        echo "Test B | N=$NUM_WORKERS | Run $RUN | Energy: $ENERGY_J J"

        # 3f. Parse timing
        grep "PERF_SUMMARY" "$LOG_FILE" || true

        # 3g. Auto-parse PERF_SUMMARY and record to CSV
        PERF_LINE=$(grep "PERF_SUMMARY" "$LOG_FILE" | head -1 || true)
        if [ -n "$PERF_LINE" ]; then
            CSV_LINE="$RUN,$NUM_WORKERS,$ENERGY_J,$(echo "$PERF_LINE" | awk '{print $5","$6","$7","$8","$9","$10}')"
        else
            CSV_LINE="$RUN,$NUM_WORKERS,$ENERGY_J,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR"
        fi
        
        echo "$CSV_LINE" >> "$CSV_FILE"

        # ---------------------------------------------------------
        # Save individual combined file for this specific run
        # ---------------------------------------------------------
        COMBINED_FILE="combined_test_b_rank_${NUM_WORKERS}_run_${RUN}.txt"
        
        echo "================ LOG FILE CONTENT ================" > "$COMBINED_FILE"
        cat "$LOG_FILE" >> "$COMBINED_FILE" 2>/dev/null || echo "Log file not found." >> "$COMBINED_FILE"
        echo "" >> "$COMBINED_FILE"
        echo "================ CSV LINE ADDED ================" >> "$COMBINED_FILE"
        echo "$CSV_LINE" >> "$COMBINED_FILE"

        echo "Saved combined output to: $COMBINED_FILE"

        sleep 2
    done
done

echo ""
echo "============================================================"
echo "Automated testing complete."
echo "All results appended to $CSV_FILE"
echo "Individual run files saved as combined_test_b_rank_*_run_*.txt"
echo "============================================================"

# Terminal bell to notify user
echo -e "\a"

