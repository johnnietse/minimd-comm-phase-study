#!/bin/bash

# Launch MPI job in background
mpirun --oversubscribe -np 32 ./miniMD_openmpi -i in.lj.miniMD &
MPI_PID=$!

echo "mpirun PID: $MPI_PID"

# Give MPI ranks time to spawn
sleep 2

# Start monitoring
./monitor_mpi_cores.sh miniMD_openmpi &
MONITOR_PID=$!

# Wait for MPI job to finish
wait $MPI_PID

# Stop monitoring
kill $MONITOR_PID 2>/dev/null

echo "Run complete."
