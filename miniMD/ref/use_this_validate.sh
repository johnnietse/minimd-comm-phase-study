#!/bin/bash

# ================= CONFIGURATION =================
MPI_RANKS=${1:-8}
PYTHON_SCRIPT="test.py"
INPUT_FILE="in.lj.miniMD"
RAPL_FILE="/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE="/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

# Frequencies
RESET_FREQ="2000000"
TOTAL_CORES=32

echo "=========================================================="
echo "   CAPSTONE ENERGY TEST: NON-SEQUENTIAL CORES"
echo "=========================================================="

trap "pkill -f $PYTHON_SCRIPT; rm -f host_rankfile; exit" INT TERM EXIT

# ================= 1. SMART CORE PARSER =================
# Returns: "worker_core_1,worker_core_2...|monitor_core"
get_exact_core_list() {
    local needed_ranks=$1
    # parsing 'taskset -cp' output which looks like "pid 1234's current affinity list: 0-3,6"
    taskset -cp $$ | awk -F': ' '{print $2}' | python3 -c "
import sys

def parse_ranges(r):
    # Turns '0-2,5,7-9' into [0,1,2,5,7,8,9]
    cores = []
    if not r: return []
    for part in r.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e + 1))
        else:
            cores.append(int(part))
    return sorted(list(set(cores)))

try:
    input_str = sys.stdin.read().strip()
    all_cores = parse_ranges(input_str)
    needed = int($needed_ranks)

    # We need (Ranks + 1 Monitor) cores total
    if len(all_cores) < (needed + 1):
        print(f'ERROR_NOT_ENOUGH_CORES:{len(all_cores)}', file=sys.stderr)
        sys.exit(1)

    # Pop the LAST available core for the monitor
    mon_core = all_cores.pop()

    # Take the first N available cores for workers
    worker_cores = all_cores[:needed]

    # Return CSV format
    print(f\"{','.join(map(str, worker_cores))}|{mon_core}\")
except Exception as e:
    sys.exit(1)
"
}

# ================= UTILS =================
reset_all_cores() {
    # Reset everything to 2.0GHz to start fresh
    for (( core=0; core<TOTAL_CORES; core++ )); do
        local gov_file="/sys/devices/system/cpu/cpu$core/cpufreq/scaling_governor"
        local speed_file="/sys/devices/system/cpu/cpu$core/cpufreq/scaling_setspeed"
        if [ -w "$gov_file" ]; then
            echo "userspace" > "$gov_file" 2>/dev/null
            echo "$RESET_FREQ" > "$speed_file" 2>/dev/null
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
    python3 -c "print($END_VAL - $START_VAL if ($END_VAL - $START_VAL) >= 0 else ($END_VAL - $START_VAL) + $MAX_VAL)"
    rm -f start_snapshot.tmp
}

# ================= BENCHMARK LOOP =================
run_benchmark() {
    local mode=$1

    reset_all_cores

    # 1. PARSE ACTUAL AVAILABLE CORES
    local ALLOC_STR=$(get_exact_core_list $MPI_RANKS)
    if [[ $ALLOC_STR == "ERROR"* ]]; then
        echo "Not enough cores available!" >&2
        exit 1
    fi

    local WORKER_CORES_CSV=$(echo "$ALLOC_STR" | cut -d'|' -f1)
    local MONITOR_CORE=$(echo "$ALLOC_STR" | cut -d'|' -f2)

    # 2. GENERATE OPENMPI RANKFILE
    # This maps Rank 0 -> First Worker Core, Rank 1 -> Second Worker Core, etc.
    # It completely ignores sequential logic and uses your EXACT list.
    rm -f host_rankfile
    local rank=0
    IFS=',' read -ra ADDR <<< "$WORKER_CORES_CSV"
    for core in "${ADDR[@]}"; do
        echo "rank $rank=localhost slot=$core" >> host_rankfile
        ((rank++))
    done

    

    # ================= BASELINE LOGIC =================
    if [ "$mode" == "BASELINE" ]; then
        # =======================================================
        #  OPTIMIZATION: FORCE MPI TO YIELD (SLEEP) WHEN WAITING
        # =======================================================
        # 1. Tell OpenMPI to yield the processor when idle
        export OMPI_MCA_mpi_yield_when_idle=0
        
        # 2. Adjust the pause count to yield sooner (optional but recommended)
        export OMPI_MCA_opal_progress_yield_when_idle=0

        # 3. If you use OpenMP threads inside MPI ranks:
        export OMP_WAIT_POLICY=ACTIVE
        export OMP_PROC_BIND=false

        # ---------------------------------------------------------
        # 2. FORCE HARDWARE AWAKE (THE HEATER TRICK)
        # ---------------------------------------------------------
        # We launch a low-priority infinite loop on every worker core.
        # It only runs when miniMD pauses for I/O, forcing the CPU 
        # to stay at 100% Util / 2.0 GHz instead of sleeping.
        # HEATER_PIDS=""
        # IFS=',' read -ra CORES <<< "$WORKER_CORES_CSV"
        # for core in "${CORES[@]}"; do
        #     # nice -n 19 means "lowest priority" -> won't slow down miniMD
        #     taskset -c $core nice -n 19 python3 -c 'while True: pass' > /dev/null 2>&1 & 
        #     HEATER_PIDS="$HEATER_PIDS $!"
        # done
        
        start_energy_monitor
        local start_t=$(date +%s.%N)

        # Use Rankfile to force binding on the exact cores we own
        export OMP_NUM_THREADS=1
        mpirun -np $MPI_RANKS \
            --rankfile host_rankfile \
            --report-bindings \
            ./miniMD_openmpi -i $INPUT_FILE > run_baseline.log 2>&1

        local energy_raw=$(stop_energy_monitor)
        local end_t=$(date +%s.%N)

    # ================= MONITORED LOGIC =================
    elif [ "$mode" == "MONITORED" ]; then
        # =======================================================
        #  OPTIMIZATION: FORCE MPI TO YIELD (SLEEP) WHEN WAITING
        # =======================================================
         # 1. Tell OpenMPI to yield the processor when idle
        export OMPI_MCA_mpi_yield_when_idle=0
        
        # 2. Adjust the pause count to yield sooner (optional but recommended)
        export OMPI_MCA_opal_progress_yield_when_idle=0

        # 3. If you use OpenMP threads inside MPI ranks:
        export OMP_WAIT_POLICY=ACTIVE
        export OMP_PROC_BIND=false

        #  # ---------------------------------------------------------
        # # 2. FORCE HARDWARE AWAKE (THE HEATER TRICK)
        # # ---------------------------------------------------------
        # # We launch a low-priority infinite loop on every worker core.
        # # It only runs when miniMD pauses for I/O, forcing the CPU 
        # # to stay at 100% Util / 2.0 GHz instead of sleeping.
        # HEATER_PIDS=""
        # IFS=',' read -ra CORES <<< "$WORKER_CORES_CSV"
        # for core in "${CORES[@]}"; do
        #     # nice -n 19 means "lowest priority" -> won't slow down miniMD
        #     taskset -c $core nice -n 19 python3 -c 'while True: pass' > /dev/null 2>&1 & 
        #     HEATER_PIDS="$HEATER_PIDS $!"
        # done

        # 1. Start Monitor on the explicit MONITOR_CORE
        #    Pass the CSV list of workers so Python knows exactly who to watch
        taskset -c $MONITOR_CORE python3 -u $PYTHON_SCRIPT --heartbeat --cores "$WORKER_CORES_CSV" > monitor_output.log 2>&1 &
        MON_PID=$!

        # Wait until monitor prints readiness (warmup complete)
        timeout 10 bash -c 'until grep -q "\[MONITOR_READY\]" monitor_output.log; do sleep 0.1; done'

        start_energy_monitor
        local start_t=$(date +%s.%N)

        # 2. Run MPI with Rankfile
        export OMP_NUM_THREADS=1
        mpirun -np $MPI_RANKS \
            --rankfile host_rankfile \
            --report-bindings \
            ./miniMD_openmpi -i $INPUT_FILE > run_monitored.log 2>&1

        local end_t=$(date +%s.%N)
        local energy_raw=$(stop_energy_monitor)

        kill -SIGINT $MON_PID 2>/dev/null
        wait $MON_PID 2>/dev/null
    fi

    # ================= CALCULATION =================
    local energy_joules=$(echo "$energy_raw / 1000000" | bc -l)
    local time_diff=$(echo "$end_t - $start_t" | bc -l)
    echo "$time_diff $energy_joules"
}

# ================= RUN =================

echo "1. Running Baseline..."
read base_time base_energy <<< $(run_benchmark "BASELINE")
echo "   Baseline:  ${base_time}s | ${base_energy}J"

echo "2. Running Monitored..."
read mon_time mon_energy <<< $(run_benchmark "MONITORED")
echo "   Monitored: ${mon_time}s | ${mon_energy}J"

# ================= RESULTS =================
if (( $(echo "$base_time > 0" | bc -l) )); then
    time_pct=$(echo "(($mon_time - $base_time) / $base_time) * 100" | bc -l)
    saved_j=$(echo "$base_energy - $mon_energy" | bc -l)
    energy_pct=$(echo "(($base_energy - $mon_energy) / $base_energy) * 100" | bc -l)

    printf "\n================ RESULTS ================\n"
    printf "Runtime Impact: %+.2f%%\n" $time_pct
    printf "Energy Savings: %s Joules (%.2f%%)\n" $saved_j $energy_pct
else
    echo "Error: Baseline time was 0"
fi
