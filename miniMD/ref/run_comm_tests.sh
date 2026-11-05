#!/usr/bin/env bash
# communication_phase_tests_safe.sh
# HPC-friendly Communication Phase Testing for miniMD
# - Parses miniMD output correctly
# - Uses RAPL sysfs only if present (no perf energy events)
# - Produces baseline_results.csv, optimized_results.csv, weak_scaling.csv, and summary

set -euo pipefail
IFS=$'\n\t'

### ---------- CONFIGURATION ----------
MPI_PROCESSES=${MPI_PROCESSES:-32}
OMP_THREADS=${OMP_THREADS:-1}
TEST_RUNS=${TEST_RUNS:-5}            # reduced default for quick tests; increase as needed
MINIMD_PATH=${MINIMD_PATH:-"./miniMD_openmpi"}
INPUT_FILE=${INPUT_FILE:-"in.lj.miniMD"}
OUTPUT_DIR=${OUTPUT_DIR:-"communication_test_results"}
LOG_INTERVAL_MS=${LOG_INTERVAL_MS:-500}
USE_RAPL=1                           # 1 = try to use RAPL sysfs energy readings, 0 = skip
CPUPWR_OK=0                          # will detect at runtime
# MPI_LAUNCH=${MPI_LAUNCH:-"mpirun --oversubscribe -np"}  # adjust if your cluster uses srun
MPI_LAUNCH="mpirun"



mkdir -p "$OUTPUT_DIR"

echo "Comm phase tests starting"
echo "MPI processes: $MPI_PROCESSES, OMP threads: $OMP_THREADS, runs: $TEST_RUNS"
echo "Output dir: $OUTPUT_DIR"
echo ""

### ---------- HELPERS ----------
log() { echo "[$(date '+%F %T')] $*"; }

check_requirements() {
    if [ ! -x "$MINIMD_PATH" ]; then
        log "ERROR: miniMD binary not found or not executable at: $MINIMD_PATH"
        exit 1
    fi

    # detect cpupower presence (but do not require it)
    if command -v cpupower &>/dev/null; then
        CPUPWR_OK=1
    fi

    # detect RAPL sysfs
    if [ ! -d "/sys/class/powercap/intel-rapl" ]; then
        log "RAPL sysfs not present. Energy measurement via /sys/class/powercap will be disabled."
        USE_RAPL=0
    fi

    # minimal sanity: mpi launcher present
    if ! command -v mpirun &>/dev/null && ! command -v srun &>/dev/null; then
        log "WARNING: neither mpirun nor srun found in PATH. You may need to load MPI module."
    fi
}

# safe read of rapl microjoules; returns integer microjoules or empty on failure
rapl_read_pkg() {
    if [ "$USE_RAPL" -eq 1 ]; then
        # choose first package energy file found
        local f
        f=$(find /sys/class/powercap/intel-rapl -maxdepth 2 -type f -name energy_uj 2>/dev/null | head -n1 || true)
        if [ -n "$f" ] && [ -r "$f" ]; then
            cat "$f"
            return 0
        fi
    fi
    echo ""
}

# Extract the numeric miniMD performance line (the line after "Performance Summary")
# Returns the line or empty
extract_perf_line() {
    local outfile="$1"
    # find the line number containing Performance Summary (match case-insensitive)
    local ln
    ln=$(grep -n -i "Performance Summary" "$outfile" 2>/dev/null | head -n1 | cut -d: -f1 || true)
    if [ -n "$ln" ]; then
        # the numeric line is expected right after the header; skip blank lines
        local candidate
        candidate=$(sed -n "$((ln+1)),$((ln+4))p" "$outfile" | sed '/^\s*#/d' | sed -n '1p' || true)
        echo "$candidate"
        return
    fi

    # fallback: find a line with many numeric columns (a heuristic)
    grep -E '^[[:space:]]*[0-9]+' "$outfile" | awk 'NF>=8 {print; exit}' || true
}

# parse total_time and t_comm from perf_line; prints "total_time comm_time" or " " if not found
parse_times_from_perf_line() {
    local line="$1"
    if [ -z "$line" ]; then
        echo ""
        return
    fi
    # miniMD numeric ordering: MPI_proc OMP_threads nsteps natoms t_total t_force t_neigh t_comm ...
    # t_total is field 5, t_comm is field 8
    # use awk to be robust to extra spaces
    local total_time
    local comm_time
    total_time=$(echo "$line" | awk '{print $5}')
    comm_time=$(echo "$line" | awk '{print $8}')
    # validate numeric using regex
    if [[ "$total_time" =~ ^[0-9]+(\.[0-9]+)?$ ]] && [[ "$comm_time" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        echo "$total_time $comm_time"
    else
        echo ""
    fi
}

# run a single miniMD invocation and capture outputs; args: run_label output_prefix
run_minimd_once() {
    local runlabel="$1"
    local outprefix="$2"
    local out="$OUTPUT_DIR/${outprefix}_output_${runlabel}.txt"
    local err="$OUTPUT_DIR/${outprefix}_stderr_${runlabel}.txt"

    export OMP_NUM_THREADS="$OMP_THREADS"
    # run the app; use MPI_LAUNCH variable
     

    $MPI_LAUNCH --oversubscribe -np "$MPI_PROCESSES" "$MINIMD_PATH" -i "$INPUT_FILE" > "$out" 2> "$err" || {
        log "miniMD run ($runlabel) exited with non-zero status; check $err"
    }
    echo "$out"
}

### ---------- TESTS ----------
run_baseline_test() {
    local run_id="$1"
    log "Baseline run $run_id: starting"
    # record rapl before
    local rapl_before=""
    if [ "$USE_RAPL" -eq 1 ]; then
        rapl_before=$(rapl_read_pkg)
    fi

    local outfile
    outfile=$(run_minimd_once "$run_id" "baseline")

    # rapl after
    local rapl_after=""
    if [ "$USE_RAPL" -eq 1 ]; then
        rapl_after=$(rapl_read_pkg)
    fi

    # parse miniMD perf numbers
    local perf_line
    perf_line=$(extract_perf_line "$outfile" || true)
    local times
    times=$(parse_times_from_perf_line "$perf_line" || true)
    local total_time="" comm_time="" energy_j=""
    if [ -n "$times" ]; then
        total_time=$(echo "$times" | awk '{print $1}')
        comm_time=$(echo "$times" | awk '{print $2}')
    fi

    if [ -n "$rapl_before" ] && [ -n "$rapl_after" ]; then
        # rapl values are microjoules; compute delta J
        local delta_uj=$((rapl_after - rapl_before))
        # handle wrap-around (typical for energy_uj counters)
        if [ "$delta_uj" -lt 0 ]; then
            # naive wrap handling: try large positive
            delta_uj=$((delta_uj + 4294967296))
        fi
        # convert to joules (microjoules -> J)
        energy_j=$(awk "BEGIN {printf \"%.6f\", $delta_uj/1000000}")
    fi

    # ensure empty fields are represented as NA
    total_time=${total_time:-NA}
    comm_time=${comm_time:-NA}
    energy_j=${energy_j:-NA}

    echo "$run_id,$total_time,$comm_time,$energy_j" >> "$OUTPUT_DIR/baseline_results.csv"
    log "Baseline run $run_id: done (total_time=$total_time, comm_time=$comm_time, energy_j=$energy_j)"
}

run_optimized_test() {
    local run_id="$1"
    log "Optimized run $run_id: starting"

    # NOTE: We keep optimization conservative and simple because many clusters disallow frequency changes.
    # If cpupower exists and user is allowed, we attempt a mild cap during runtime; otherwise skip.
    if [ "$CPUPWR_OK" -eq 1 ]; then
        log "cpupower available: attempting conservative governor shift (may require privileges)"
        # try to set conservative target; errors are ignored
        cpupower frequency-set -g powersave >/dev/null 2>&1 || true
    else
        log "cpupower not available or not in PATH; optimizer will not change CPU freq."
    fi

    local rapl_before=""
    if [ "$USE_RAPL" -eq 1 ]; then
        rapl_before=$(rapl_read_pkg)
    fi

    local outfile
    outfile=$(run_minimd_once "$run_id" "optimized")

    local rapl_after=""
    if [ "$USE_RAPL" -eq 1 ]; then
        rapl_after=$(rapl_read_pkg)
    fi

    # restore cpupower (best-effort)
    if [ "$CPUPWR_OK" -eq 1 ]; then
        cpupower frequency-set -g performance >/dev/null 2>&1 || true
    fi

    # parse times
    local perf_line
    perf_line=$(extract_perf_line "$outfile" || true)
    local times
    times=$(parse_times_from_perf_line "$perf_line" || true)
    local total_time="" comm_time="" energy_j=""
    if [ -n "$times" ]; then
        total_time=$(echo "$times" | awk '{print $1}')
        comm_time=$(echo "$times" | awk '{print $2}')
    fi

    if [ -n "$rapl_before" ] && [ -n "$rapl_after" ]; then
        local delta_uj=$((rapl_after - rapl_before))
        if [ "$delta_uj" -lt 0 ]; then
            delta_uj=$((delta_uj + 4294967296))
        fi
        energy_j=$(awk "BEGIN {printf \"%.6f\", $delta_uj/1000000}")
    fi

    total_time=${total_time:-NA}
    comm_time=${comm_time:-NA}
    energy_j=${energy_j:-NA}

    echo "$run_id,$total_time,$comm_time,$energy_j" >> "$OUTPUT_DIR/optimized_results.csv"
    log "Optimized run $run_id: done (total_time=$total_time, comm_time=$comm_time, energy_j=$energy_j)"
}

run_weak_scaling_analysis() {
    log "Starting weak scaling analysis"
    local proc_counts=(4 8 16 32)
    local base_atoms=32768

    echo "processes,atoms_per_proc,total_time,comm_time,energy_j" > "$OUTPUT_DIR/weak_scaling.csv"

    for p in "${proc_counts[@]}"; do
        # compute scaled atoms per proc (keep integer)
        local total_atoms=$((base_atoms * p / 4))
        local atoms_per_proc=$((total_atoms / p))

        log "Weak-scaling: running with $p procs, atoms_per_proc=$atoms_per_proc (total_atoms=$total_atoms)"

        # Optionally adjust input file; here we assume in.lj.miniMD uses the "size" field 32 32 32
        # We'll copy the input and not edit in-place to avoid modifying original
        local tmp_input="$OUTPUT_DIR/in.${p}.lj.miniMD"
        cp "$INPUT_FILE" "$tmp_input" || true
        # (You can edit $tmp_input if you have a mapping from atoms->grid)

        export OMP_NUM_THREADS="$OMP_THREADS"
        $MPI_LAUNCH "$p" "$MINIMD_PATH" -i "$tmp_input" > "$OUTPUT_DIR/weak_${p}_output.txt" 2> "$OUTPUT_DIR/weak_${p}_stderr.txt" || log "weak-scaling run p=$p finished with non-zero exit"

        local perf_line
        perf_line=$(extract_perf_line "$OUTPUT_DIR/weak_${p}_output.txt" || true)
        local times
        times=$(parse_times_from_perf_line "$perf_line" || true)
        local total_time="NA" comm_time="NA"
        if [ -n "$times" ]; then
            total_time=$(echo "$times" | awk '{print $1}')
            comm_time=$(echo "$times" | awk '{print $2}')
        fi

        echo "$p,$atoms_per_proc,$total_time,$comm_time,NA" >> "$OUTPUT_DIR/weak_scaling.csv"
    done
    log "Weak scaling analysis complete"
}

### ---------- MAIN ----------
check_requirements

# initialize CSVs (columns adapt to whether energy is available)
if [ "$USE_RAPL" -eq 1 ]; then
    echo "run_id,total_time,comm_time,energy_j" > "$OUTPUT_DIR/baseline_results.csv"
    echo "run_id,total_time,comm_time,energy_j" > "$OUTPUT_DIR/optimized_results.csv"
else
    echo "run_id,total_time,comm_time,energy_j" > "$OUTPUT_DIR/baseline_results.csv"
    echo "run_id,total_time,comm_time,energy_j" > "$OUTPUT_DIR/optimized_results.csv"
    log "Energy column will contain NA since RAPL not available"
fi

log "Phase 1: Baseline measurements"
for ((i=1;i<=TEST_RUNS;i++)); do
    run_baseline_test "$i"
done

log "Phase 2: Optimized measurements"
for ((i=1;i<=TEST_RUNS;i++)); do
    run_optimized_test "$i"
done

log "Phase 3: Additional analyses"
run_weak_scaling_analysis

log "Phase 4: Summary generation"

python3 - "$OUTPUT_DIR" <<'PYEOF'
import sys, pandas as pd, numpy as np
outdir = sys.argv[1]
try:
    b = pd.read_csv(f"{outdir}/baseline_results.csv")
    o = pd.read_csv(f"{outdir}/optimized_results.csv")
    # convert numeric where possible, coerce errors to NaN
    for df in (b,o):
        df['total_time'] = pd.to_numeric(df['total_time'], errors='coerce')
        df['comm_time']  = pd.to_numeric(df['comm_time'], errors='coerce')
        df['energy_j']   = pd.to_numeric(df['energy_j'], errors='coerce')

    baseline_avg = b.mean(numeric_only=True)
    opt_avg = o.mean(numeric_only=True)
    improvements = (baseline_avg - opt_avg) / baseline_avg * 100

    with open(f"{outdir}/summary_report.txt",'w') as f:
        f.write("COMMUNICATION PHASE TESTING SUMMARY\n")
        f.write("===================================\n\n")
        f.write(f"Configuration: {len(b)} baseline runs, {len(o)} optimized runs\n\n")
        f.write("Average Results (NA means not available)\n")
        f.write("Metric            | Baseline     | Optimized    | Improvement (%)\n")
        f.write("------------------|--------------|--------------|----------------\n")
        def fmt(k):
            a = baseline_avg.get(k, np.nan)
            b_ = opt_avg.get(k, np.nan)
            imp = improvements.get(k, np.nan)
            return f"{k:16} | {a:12.6g} | {b_:12.6g} | {imp:+10.2f}\n"
        for key in ['total_time','comm_time','energy_j']:
            f.write(fmt(key))
        f.write("\nNotes:\n")
        f.write("- total_time and comm_time parsed from miniMD Performance Summary numeric line.\n")
        if pd.isna(baseline_avg.get('energy_j')):
            f.write("- Energy measurement not available (RAPL sysfs missing) or failed; energy_j fields may be NA.\n")
        else:
            f.write("- Energy values are measured via RAPL sysfs delta (microJ -> J).\n")
except Exception as e:
    with open(f"{outdir}/summary_report.txt",'w') as f:
        f.write("Error generating summary: " + str(e) + "\n")
PYEOF

cat "$OUTPUT_DIR/summary_report.txt"
log "All done. Results in $OUTPUT_DIR"
