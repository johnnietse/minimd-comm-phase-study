#!/bin/bash
# ================================================================
# Frequency Testing Script for miniMD Communication Phase
# ================================================================
# Tests miniMD at three frequencies: 1.2 GHz, 1.6 GHz, 2.0 GHz
# Collects performance and energy data for each frequency.
#
# Core Layout (32-core node):
#   Core 31: RESERVED (HPC maintenance, no frequency control permission)
#   Core 30: MONITOR  (Python frequency controller runs here)
#   Cores 0–29: WORKERS (MPI processes, 1:1 core-binding)
#   Valid worker counts: 1, 2, 4, 8, 16, 30
#
# Usage:
#   chmod +x run_freq_tests.sh
#   ./run_freq_tests.sh              # default: 16 worker cores
#   ./run_freq_tests.sh 8            # 8 worker cores
#   ./run_freq_tests.sh 30           # max 30 worker cores
#
# Prerequisites:
#   - Slurm allocation with exclusive access
#   - miniMD binary built with communication phase
#   - Write access to cpufreq sysfs (cores 0-30 only)
# ================================================================

set -e

# =================== CONFIGURATION ===================
MINIMD_BIN="./miniMD_openmpi"
INPUT_FILE="in.lj.miniMD"
OUTPUT_DIR="freq_test_results"
CKPT_DIR="chk"
COMM_STANDIN_MB=309         # Stand-in communication size

# Number of worker cores — from CLI arg or default to 16
NUM_WORKERS=${1:-16}

# Validate worker count
VALID_COUNTS="1 2 4 8 16 30"
is_valid=0
for v in $VALID_COUNTS; do
    if [ "$NUM_WORKERS" = "$v" ]; then
        is_valid=1
        break
    fi
done

if [ "$is_valid" = "0" ]; then
    echo "ERROR: Invalid worker count: $NUM_WORKERS"
    echo "Valid counts: $VALID_COUNTS"
    echo "Usage: $0 [num_workers]"
    exit 1
fi

# Core layout:
#   Workers:  cores 0 to (NUM_WORKERS-1) → each bound to 1 MPI process
#   Monitor:  core 30 (for frequency controller)
#   Reserved: core 31 (HPC maintenance, cannot change freq)
# MPI ranks = NUM_WORKERS (1:1 core-binding)
NUM_PROCS=$NUM_WORKERS
WORKER_CORES_LAST=$((NUM_WORKERS - 1))  # last worker core index

# Frequencies to test (in kHz as cpufreq expects)
FREQS=(1200000 1600000 2000000)
FREQ_LABELS=("1.2GHz" "1.6GHz" "2.0GHz")

# Number of runs per frequency for statistical significance
NUM_RUNS=3

# =================== HELPER FUNCTIONS ===================

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

set_worker_cores_freq() {
    local freq=$1
    log "Setting worker cores 0-${WORKER_CORES_LAST} to ${freq} kHz"
    for ((c=0; c<NUM_WORKERS; c++)); do
        echo "userspace" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor 2>/dev/null || true
        echo "${freq}" > /sys/devices/system/cpu/cpu${c}/cpufreq/scaling_setspeed 2>/dev/null || true
    done
    # Also set monitor core 30 (if not a worker)
    if [ "$NUM_WORKERS" -lt 30 ]; then
        echo "userspace" > /sys/devices/system/cpu/cpu30/cpufreq/scaling_governor 2>/dev/null || true
        echo "${freq}" > /sys/devices/system/cpu/cpu30/cpufreq/scaling_setspeed 2>/dev/null || true
    fi
    # NOTE: Core 31 is NEVER touched — reserved, no permission
}

verify_freq() {
    local expected=$1
    local actual
    actual=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo "0")
    if [ "$actual" = "$expected" ]; then
        log "  Verified: core 0 running at ${actual} kHz"
    else
        log "  WARNING: expected ${expected} kHz, got ${actual} kHz"
    fi
}

cleanup_checkpoint() {
    if [ -d "$CKPT_DIR" ]; then
        rm -rf "$CKPT_DIR"
        log "  Cleaned up checkpoint directory"
    fi
    rm -f phase_marker.txt
}

# =================== MAIN ===================

# Create output directory
mkdir -p "$OUTPUT_DIR"

log "================================================================"
log "miniMD Communication Phase Frequency Testing"
log "================================================================"
log "Binary:       $MINIMD_BIN"
log "Workers:      $NUM_WORKERS (cores 0-${WORKER_CORES_LAST})"
log "MPI procs:    $NUM_PROCS (1:1 core-binding)"
log "Monitor core: 30"
log "Reserved:     core 31 (no permission)"
log "Comm stand-in: ${COMM_STANDIN_MB} MB"
log "Frequencies:  ${FREQ_LABELS[*]}"
log "Runs per freq: $NUM_RUNS"
log "Output dir:   $OUTPUT_DIR"
log "================================================================"
log ""

# Summary CSV
SUMMARY_FILE="${OUTPUT_DIR}/summary_${NUM_WORKERS}workers.csv"
echo "num_workers,frequency_ghz,run,total_time_s,force_time_s,neigh_time_s,comm_time_s,other_time_s,perf_atoms_step_s,comm_phase_duration_s,comm_bandwidth_mbs" > "$SUMMARY_FILE"

for idx in "${!FREQS[@]}"; do
    freq=${FREQS[$idx]}
    label=${FREQ_LABELS[$idx]}

    log "============================================"
    log "Testing at ${label} (${freq} kHz) with ${NUM_WORKERS} workers"
    log "============================================"

    # Set frequency on worker cores only (skip core 31)
    set_worker_cores_freq "$freq"
    verify_freq "$freq"

    for run in $(seq 1 $NUM_RUNS); do
        log ""
        log "--- Run ${run}/${NUM_RUNS} at ${label}, ${NUM_WORKERS} workers ---"

        # Clean up from previous run
        cleanup_checkpoint

        # Output file for this run
        RUN_OUTPUT="${OUTPUT_DIR}/${NUM_WORKERS}w_${label}_run${run}.log"

        # Run miniMD with core-binding
        log "  Starting miniMD with ${NUM_PROCS} MPI ranks on cores 0-${WORKER_CORES_LAST}..."
        mpirun --oversubscribe -np ${NUM_PROCS} \
            --bind-to core \
            --map-by core \
            ${MINIMD_BIN} \
            -i ${INPUT_FILE} \
            --comm_standin_mb ${COMM_STANDIN_MB} \
            2>&1 | tee "${RUN_OUTPUT}"

        # Parse results from output
        PERF_LINE=$(grep "PERF_SUMMARY" "${RUN_OUTPUT}" || echo "")
        if [ -n "$PERF_LINE" ]; then
            t_total=$(echo "$PERF_LINE" | awk '{print $5}')
            t_force=$(echo "$PERF_LINE" | awk '{print $6}')
            t_neigh=$(echo "$PERF_LINE" | awk '{print $7}')
            t_comm=$(echo "$PERF_LINE" | awk '{print $8}')
            t_other=$(echo "$PERF_LINE" | awk '{print $9}')
            perf=$(echo "$PERF_LINE" | awk '{print $10}')
        else
            t_total="N/A"; t_force="N/A"; t_neigh="N/A"
            t_comm="N/A"; t_other="N/A"; perf="N/A"
        fi

        # Parse communication phase duration
        COMM_LINE=$(grep "Duration:" "${RUN_OUTPUT}" | head -1 || echo "")
        comm_dur=$(echo "$COMM_LINE" | awk '{print $2}')
        [ -z "$comm_dur" ] && comm_dur="N/A"

        BW_LINE=$(grep "Effective bandwidth:" "${RUN_OUTPUT}" | head -1 || echo "")
        comm_bw=$(echo "$BW_LINE" | awk '{print $3}')
        [ -z "$comm_bw" ] && comm_bw="N/A"

        # Append to summary
        freq_ghz=$(echo "scale=1; $freq/1000000" | bc)
        echo "${NUM_WORKERS},${freq_ghz},${run},${t_total},${t_force},${t_neigh},${t_comm},${t_other},${perf},${comm_dur},${comm_bw}" >> "$SUMMARY_FILE"

        log "  Results: total=${t_total}s, perf=${perf} atoms*steps/s"
        log "  Comm phase: ${comm_dur}s, BW: ${comm_bw} MB/s"
    done
done

# Restore worker cores to max frequency
log ""
log "Restoring worker cores 0-${WORKER_CORES_LAST} to max frequency..."
set_worker_cores_freq 2000000

# Print summary
log ""
log "================================================================"
log "TEST COMPLETE — ${NUM_WORKERS} workers"
log "================================================================"
log "Results saved to: ${OUTPUT_DIR}/"
log "Summary CSV: ${SUMMARY_FILE}"
log ""
log "--- Summary ---"
cat "$SUMMARY_FILE" | column -t -s','
log ""
log "To test other worker counts:"
log "  $0 1    # 1 worker"
log "  $0 2    # 2 workers"
log "  $0 4    # 4 workers"
log "  $0 8    # 8 workers"
log "  $0 16   # 16 workers (max power of 2)"
log "  $0 30   # 30 workers (max available)"
log "================================================================"
