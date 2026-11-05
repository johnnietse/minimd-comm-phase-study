#!/bin/bash

# ================= CONFIGURATION =================
MPI_RANKS=${1:-8}
PYTHON_SCRIPT="test.py"
INPUT_FILE="in.lj.miniMD"
RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

echo "=========================================================="
echo "   CAPSTONE ENERGY TEST: BASELINE vs. PYTHON MONITOR"
echo "   Ranks: $MPI_RANKS | Input: $INPUT_FILE"
echo "=========================================================="

trap "pkill -f $PYTHON_SCRIPT; exit" INT TERM EXIT

# ================= HELPER FUNCTIONS =================
get_expanded_cores() {
    taskset -cp $$ | awk -F': ' '{print $2}' | python3 -c '
import sys
try:
    r = sys.stdin.read().strip()
    cores = []
    if r:
        for part in r.split(","):
            if "-" in part:
                s, e = map(int, part.split("-"))
                cores.extend(range(s, e + 1))
            else:
                cores.append(int(part))
    print(",".join(map(str, cores)))
except:
    print("")
'
}

set_max_freq() {
    MY_CORES_LIST=$(get_expanded_cores)
    IFS=',' read -ra CORES <<< "$MY_CORES_LIST"
    for core in "${CORES[@]}"; do
        freq_file="/sys/devices/system/cpu/cpu$core/cpufreq/scaling_setspeed"
        if [ -w "$freq_file" ]; then
            echo 2000000 > "$freq_file" 2>/dev/null
        fi
    done
}

start_energy_monitor() {
    cat "$RAPL_FILE" > start_snapshot.tmp 2>/dev/null || echo "0" > start_snapshot.tmp
}

stop_energy_monitor() {
    local END_VAL=$(cat "$RAPL_FILE" 2>/dev/null || echo 0)
    local START_VAL=$(cat start_snapshot.tmp 2>/dev/null || echo 0)
    local MAX_VAL=$(cat "$MAX_RAPL_FILE" 2>/dev/null || echo 262143328850)

    python3 -c "
s = int('$START_VAL')
e = int('$END_VAL')
m = int('$MAX_VAL')
diff = e - s
if diff < 0: diff += m
print(diff)
"
    rm -f start_snapshot.tmp
}

# ================= RUN LOGIC =================

run_benchmark() {
    local mode=$1
    set_max_freq
    
    # --- ISOLATION LOGIC ---
    ALL_CORES=$(get_expanded_cores)
    # 1. Pick the last core for Python
    MONITOR_CORE=$(echo $ALL_CORES | awk -F',' '{print $NF}')
    # 2. Give MPI everything else
    WORKER_CORES=$(echo $ALL_CORES | sed "s/,$MONITOR_CORE//")
    # -----------------------

    start_energy_monitor
    local start_t=$(date +%s.%N)
    
    if [ "$mode" == "BASELINE" ]; then
        # Baseline uses WORKER_CORES so it has the same hardware resources as Monitored
        taskset -c $WORKER_CORES mpirun -np $MPI_RANKS --bind-to core ./miniMD_openmpi -i $INPUT_FILE > run_baseline.log 2>&1
        
    elif [ "$mode" == "MONITORED" ]; then
        # Monitor on isolated core (Silent Mode: No --heartbeat flag)
        taskset -c $MONITOR_CORE python3 $PYTHON_SCRIPT --cores "$WORKER_CORES" > monitor_output.log 2>&1 &
        MON_PID=$!
        
        sleep 2
        
        # MPI on worker cores
        taskset -c $WORKER_CORES mpirun -np $MPI_RANKS --bind-to core ./miniMD_openmpi -i $INPUT_FILE > run_monitored.log 2>&1
        
        kill -SIGINT $MON_PID 2>/dev/null
        wait $MON_PID 2>/dev/null
    fi
    
    local energy_raw=$(stop_energy_monitor)
    local end_t=$(date +%s.%N)

    local energy_joules=$(echo "$energy_raw / 1000000" | bc -l)
    local time_diff=$(echo "$end_t - $start_t" | bc -l)
    
    echo "$time_diff $energy_joules"
}

# ================= MAIN EXECUTION =================

echo "1. Running Baseline..."
read base_time base_energy <<< $(run_benchmark "BASELINE")
echo "   Baseline:  ${base_time}s | ${base_energy}J"

echo "2. Running Monitored..."
read mon_time mon_energy <<< $(run_benchmark "MONITORED")
echo "   Monitored: ${mon_time}s | ${mon_energy}J"

# ================= REPORT =================
echo ""
echo "================ RESULTS ================"
time_pct=$(echo "(($mon_time - $base_time) / $base_time) * 100" | bc -l)
printf "Runtime Impact: %+.2f%%\n" $time_pct

saved_j=$(echo "$base_energy - $mon_energy" | bc -l)
printf "Energy Savings: %s Joules\n" $saved_j

python3 -c "
saved = float('$saved_j')
if saved > 0: print('STATUS: SUCCESS! Energy Saved.')
else: print('STATUS: No Savings.')
"
echo "========================================="
