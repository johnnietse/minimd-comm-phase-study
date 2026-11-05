#!/bin/bash
# Monitor logical CPU usage per MPI process

PROCESS_NAME=$1
INTERVAL=1
OUTFILE="cpu_core_trace.csv"

if [ -z "$PROCESS_NAME" ]; then
  echo "Usage: $0 <process_name>"
  exit 1
fi

echo "timestamp,pid,psr,cpu_percent" > $OUTFILE

echo "Monitoring $PROCESS_NAME ..."
echo "Logging to $OUTFILE"

while pgrep $PROCESS_NAME > /dev/null; do
  TIMESTAMP=$(date +%s)

  ps -C $PROCESS_NAME -o pid=,psr=,%cpu= | while read PID PSR CPU; do
    echo "$TIMESTAMP,$PID,$PSR,$CPU" >> $OUTFILE
  done

  sleep $INTERVAL
done

echo "Monitoring finished."
