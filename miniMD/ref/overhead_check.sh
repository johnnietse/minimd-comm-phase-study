#!/bin/bash

# ================= CONFIGURATION =================
MPI_RANKS=${1:-8}
PYTHON_SCRIPT="test.py"
RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_MAX_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"
MONITOR_CORE=15
INPUT_FILE="in.lj.miniMD"

# Temp file for energy result
ENERGY_RES_FILE="energy_result.tmp"

echo "=========================================================="
echo "   CAPSTONE MANUAL TEST (INTEGRATING ENERGY MONITOR)"
echo "   Ranks: $MPI_RANKS | Input: $INPUT_FILE"
echo "=========================================================="

trap "pkill -f $PYTHON_SCRIPT; rm -f $ENERGY_RES_FILE; exit" INT TERM EXIT

# ================= HELPER FUNCTIONS =================
set_max_freq() {
    echo "   [SETUP] Resetting accessible cores to 2.0 GHz..." >&2
    {
        for freq_file in /sys/devices/system/cpu/cpu*/cpufreq/scaling_setspeed; do
            echo 2400000 > "$freq_file"
        done
    } 2>/dev/null
    sleep 1
}

# --- BACKGROUND ENERGY MONITOR (Python) ---
# This script runs in the background, sampling every 0.5s.
# It handles wrap-arounds by checking max_energy_range_uj.
start_energy_monitor() {
    rm -f $ENERGY_RES_FILE
    python3 -c "
import time, os, signal, sys

rapl_file = '$RAPL_FILE'
max_file = '$RAPL_MAX_FILE'
out_file = '$ENERGY_RES_FILE'

def read_uj():
    try:
        with open(rapl_file, 'r') as f: return int(f.read().strip())
    except: return 0

def get_max_uj():
    try:
        with open(max_file, 'r') as f: return int(f.read().strip())
    except: return 262143328850 # Fallback typical max

max_uj = get_max_uj()
last_uj = read_uj()
total_uj = 0

# Handle termination signal to write output
def finish(signum, frame):
    with open(out_file, 'w') as f:
        f.write(str(total_uj))
    sys.exit(0)

signal.signal(signal.SIGTERM, finish)
signal.signal(signal.SIGINT, finish)

while True:
    time.sleep(0.5)
    curr_uj = read_uj()
    delta = curr_uj - last_uj
    
    # Handle Wrap-Around
    if delta < 0:
        delta += max_uj
        
    total_uj += delta
    last_uj = curr_uj
" &
    ENERGY_PID=$!
}

stop_energy_monitor() {
    kill $ENERGY_PID
    wait $ENERGY_PID 2>/dev/null
    
    if [ -f "$ENERGY_RES_FILE" ]; then
        cat "$ENERGY_RES_FILE"
    else
        echo "0"
    fi
    rm -f $ENERGY_RES_FILE
}

run_benchmark() {
    local mode=$1
    local output_log="run_${mode}.log"
    
    echo "" >&2
    echo "[$mode] Starting run..." >&2
    
    set_max_freq
    
    # Start the robust energy monitor
    start_energy_monitor
    local start_t=$(date +%s.%N)
    
    if [ "$mode" == "BASELINE" ]; then
        rm -f /dev/shm/minimd_phase_hint
        unset MINIMD_HINT_MODE
        CMD="mpirun -np $MPI_RANKS --map-by core --bind-to core ./miniMD_openmpi -i $INPUT_FILE"
        
    elif [ "$mode" == "MONITORED" ]; then
        taskset -c $MONITOR_CORE python3 $PYTHON_SCRIPT --hints > monitor_debug.log 2>&1 &
        MON_PID=$!
        sleep 3 
        
        export MINIMD_HINT_MODE=1
        CMD="mpirun -np $MPI_RANKS --map-by core --bind-to core ./miniMD_openmpi -i $INPUT_FILE"
    fi

    $CMD > $output_log 2>&1
    
    local end_t=$(date +%s.%N)
    
    # Stop energy monitor and get result
    local energy_raw=$(stop_energy_monitor)

    if [ "$mode" == "MONITORED" ]; then kill $MON_PID 2>/dev/null; fi
    
    local time_diff=$(echo "$end_t - $start_t" | bc -l)
    local energy_joules=$(echo "$energy_raw / 1000000" | bc -l)

    echo "$time_diff $energy_joules"
}

# ================= EXECUTION =================

read base_time base_energy <<< $(run_benchmark "BASELINE")
echo "   Baseline:   ${base_time}s | ${base_energy}J"

read mon_time mon_energy <<< $(run_benchmark "MONITORED")
echo "   Monitored:  ${mon_time}s | ${mon_energy}J"

# ================= ANALYSIS =================
echo ""
echo "================ RESULTS ================"
if (( $(echo "$base_time > 0" | bc -l) )); then
    time_pct=$(echo "(($mon_time - $base_time) / $base_time) * 100" | bc -l)
    printf "Runtime Overhead: %+.2f%%\n" $time_pct
fi

if (( $(echo "$base_energy > 0" | bc -l) )); then
    energy_pct=$(echo "(($mon_energy - $base_energy) / $base_energy) * 100" | bc -l)
    printf "Energy Overhead:  %+.2f%%\n" $energy_pct
    
    if (( $(echo "$energy_pct < 0" | bc -l) )); then
        echo "STATUS: SUCCESS! Energy Saved."
    else
        echo "STATUS: No savings yet."
    fi
fi
echo "========================================="
