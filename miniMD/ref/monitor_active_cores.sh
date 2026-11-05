#!/bin/bash
# -------------------------------------------------------
# Monitor ACTIVE CPU CORE COUNT over time
# Works with MPI, oversubscription, Slurm, no sudo needed
# -------------------------------------------------------

INTERVAL=1
OUTFILE="active_cpu_cores.csv"
MPI_RANKS=${1:-32}

echo "timestamp,active_cpu_cores,total_logical_cpus,mpi_ranks" > "$OUTFILE"

# Read initial CPU stats
declare -A prev_idle prev_total

read_cpu_stats() {
    while read -r cpu user nice system idle iowait irq softirq steal _; do
        [[ $cpu == cpu[0-9]* ]] || continue
        total=$((user + nice + system + idle + iowait + irq + softirq + steal))
        prev_idle[$cpu]=$((idle + iowait))
        prev_total[$cpu]=$total
    done < /proc/stat
}

count_active_cores() {
    active=0
    total=0

    while read -r cpu user nice system idle iowait irq softirq steal _; do
        [[ $cpu == cpu[0-9]* ]] || continue
        total=$((total + 1))

        cur_idle=$((idle + iowait))
        cur_total=$((user + nice + system + idle + iowait + irq + softirq + steal))

        idle_delta=$((cur_idle - prev_idle[$cpu]))
        total_delta=$((cur_total - prev_total[$cpu]))

        # If CPU did *any* work, it's active
        if (( total_delta > idle_delta )); then
            active=$((active + 1))
        fi

        prev_idle[$cpu]=$cur_idle
        prev_total[$cpu]=$cur_total
    done < /proc/stat

    echo "$active,$total"
}

# Initialize
read_cpu_stats
sleep "$INTERVAL"

echo "Monitoring active CPU cores..."
echo "Logging to $OUTFILE (Ctrl+C to stop)"

while true; do
    ts=$(date +%s)
    result=$(count_active_cores)
    echo "$ts,$result,$MPI_RANKS" >> "$OUTFILE"
    sleep "$INTERVAL"
done
