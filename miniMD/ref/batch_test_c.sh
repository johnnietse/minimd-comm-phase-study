#!/bin/bash
#SBATCH --job-name=minimd_test_c
#SBATCH --output=test_c_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=30
#SBATCH --cpus-per-task=1
#SBATCH --time=10:00:00
#SBATCH --mem=32G
#SBATCH --exclusive
#SBATCH --partition=gpu-rgrant
#SBATCH --constraint=lkb

# ============================================================
# TEST C — COMMUNICATION PHASE ON + FREQUENCY CONTROLLER
#
# Runs miniMD with comm phase enabled AND the frequency controller
# (comm_freq_controller.py) running on core 30. The controller:
#   - COMPUTE: all worker cores at 2000 MHz
#   - IO:      all worker cores at 1200 MHz
#   - COMM:    core 0 at 2000 MHz, cores 1-N at 1200 MHz
#
# Core layout:
#   Cores 0-(N-1):  Workers (MPI, 1:1 binding)
#   Core 30:        Monitor (freq controller via taskset)
#   Core 31:        Reserved (no permission)
#
# Comparison:
#   B vs C → energy savings from frequency control
#   A vs C → total impact of full optimization
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
CONTROLLER="comm_freq_controller.py"
ENERGY_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_ENERGY_UJ=$(cat /sys/class/powercap/intel-rapl:0/max_energy_range_uj 2>/dev/null || echo "65532610987")
MONITOR_CORE=30

# ---- Output ----
CSV="results_test_c_${SLURM_JOB_ID}.csv"
SUMMARY_LOG="summary_test_c_${SLURM_JOB_ID}.txt"
CTRL_LOG_DIR="ctrl_logs_${SLURM_JOB_ID}"
mkdir -p "$CTRL_LOG_DIR"

echo "============================================================"
echo "TEST C — COMM PHASE ON + FREQUENCY CONTROLLER"
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $(hostname)"
echo "Date:      $(date)"
echo "MPI Counts: $MPI_COUNTS"
echo "Runs/config: $NUM_RUNS"
echo "Monitor core: $MONITOR_CORE"
echo "============================================================"

# ---- Prerequisites ----
if [ ! -f "$BINARY" ]; then
    echo "ERROR: $BINARY not found!"; exit 1
fi
if [ ! -f "$INPUT" ]; then
    echo "ERROR: $INPUT not found!"; exit 1
fi
if [ ! -f "$CONTROLLER" ]; then
    echo "ERROR: $CONTROLLER not found!"; exit 1
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

kill_controller() {
    local pid=$1
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
    fi
}

# ---- CSV Header ----
echo "run,nprocs,energy_j,avg_power_w,t_total,t_force,t_neigh,t_comm,t_other,performance,io_dur_s,io_bw_mbps,comm_dur_s,comm_bw_mbps,comm_data_mb,ctrl_transitions" > "$CSV"

# ---- Main Experiment Loop ----
TOTAL_START=$(date +%s)

for N in $MPI_COUNTS; do
    echo ""
    echo "============================================================"
    echo "  MPI Processes: $N — Starting $NUM_RUNS runs"
    echo "============================================================"

    for run in $(seq 1 $NUM_RUNS); do
        echo ""
        echo "--- N=$N, Run $run/$NUM_RUNS ---"

        # Clean checkpoint directory
        rm -rf "$CKPT_DIR"/* 2>/dev/null || true
        mkdir -p "$CKPT_DIR"

        # Clean phase marker
        rm -f phase_marker.txt

        # Reset governors to clean state before controller takes over
        reset_governors 29
        sleep 1

        # ---- Start frequency controller ----
        CTRL_LOGFILE="${CTRL_LOG_DIR}/ctrl_n${N}_run${run}.log"
        taskset -c $MONITOR_CORE python3 "$CONTROLLER" --workers $N \
            > "$CTRL_LOGFILE" 2>&1 &
        CTRL_PID=$!
        echo "  Controller started (PID=$CTRL_PID, workers=$N)"

        # Wait for controller to initialize (set governors, start polling)
        sleep 3

        TMPLOG=$(mktemp /tmp/minimd_c_${N}_${run}_XXXXXX.log)

        # Read energy BEFORE
        BEFORE_UJ=$(read_energy_uj)

        # Run miniMD — comm phase ENABLED (default)
        mpirun -np $N --map-by core --bind-to core \
            "$BINARY" -i "$INPUT" \
            2>&1 | tee "$TMPLOG"

        EXIT_CODE=$?

        # Read energy AFTER
        AFTER_UJ=$(read_energy_uj)

        # ---- Stop controller ----
        sleep 1
        kill_controller $CTRL_PID
        echo "  Controller stopped"

        # Reset governors back to performance for clean state
        reset_governors 29

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

        # Parse controller transitions
        CTRL_TRANSITIONS=$(grep -c "Phase transition:" "$CTRL_LOGFILE" 2>/dev/null || echo "0")

        # Calculate energy
        ENERGY_J=$(calc_energy_j "$BEFORE_UJ" "$AFTER_UJ")
        if [ "$(echo "$T_TOTAL > 0" | bc -l 2>/dev/null)" = "1" ]; then
            AVG_POWER=$(echo "scale=2; $ENERGY_J / $T_TOTAL" | bc -l)
        else
            AVG_POWER="0"
        fi

        # Defaults
        IO_DUR=${IO_DUR:-"N/A"}
        IO_BW=${IO_BW:-"N/A"}
        COMM_DUR=${COMM_DUR:-"N/A"}
        COMM_BW=${COMM_BW:-"N/A"}
        COMM_DATA=${COMM_DATA:-"N/A"}

        # Write CSV row
        echo "$run,$N,$ENERGY_J,$AVG_POWER,$T_TOTAL,$T_FORCE,$T_NEIGH,$T_COMM,$T_OTHER,$PERFORMANCE,$IO_DUR,$IO_BW,$COMM_DUR,$COMM_BW,$COMM_DATA,$CTRL_TRANSITIONS" >> "$CSV"

        echo "  Energy: ${ENERGY_J} J | Power: ${AVG_POWER} W | Time: ${T_TOTAL} s | Transitions: ${CTRL_TRANSITIONS}"

        rm -f "$TMPLOG"
        sleep 2
    done
done

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$(( TOTAL_END - TOTAL_START ))

# ---- Summary ----
echo ""
echo "============================================================"
echo "TEST C COMPLETE"
echo "Total wall time: $(( TOTAL_ELAPSED / 3600 ))h $(( (TOTAL_ELAPSED % 3600) / 60 ))m $(( TOTAL_ELAPSED % 60 ))s"
echo "Results: $CSV"
echo "Controller logs: $CTRL_LOG_DIR/"
echo "============================================================"

{
    echo "============================================================"
    echo "TEST C — SUMMARY (Comm phase ON + freq controller)"
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

reset_governors 29
echo "Governors restored to performance."
echo "Done."
