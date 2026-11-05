#!/bin/bash
# monitor_miniMD.sh
# Real-time monitoring of miniMD OpenMPI job resources

# Usage:
# ./monitor_miniMD.sh <process_name>
# Example: ./monitor_miniMD.sh miniMD_openmpi

PROCESS_NAME=$1

if [ -z "$PROCESS_NAME" ]; then
    echo "Usage: $0 <process_name>"
    exit 1
fi

# Check every 1 second by default
INTERVAL=1

echo "Monitoring processes matching: $PROCESS_NAME"
echo "Press Ctrl+C to stop."

while true; do
    clear
    echo "===== miniMD Resource Monitor ====="
    date
    echo "Rank | PID    | CPU% | MEM% | Core"
    echo "------------------------------------"
    
    # List all processes matching the name
    ps -C $PROCESS_NAME -o pid,cmd,%cpu,%mem,psr --sort=-%cpu | awk 'NR>1 {printf "%-4d | %-6s | %-4s | %-4s | %-4s\n", NR-1, $1, $3, $4, $5}'
    
    sleep $INTERVAL
done
