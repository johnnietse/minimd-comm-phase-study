#!/bin/bash

# Safe fallback for the cluster: restore all worker cores to maximum performance
# Prevents cores from getting permanently orphaned in "userspace" / throttled states 
# if the monitor is killed mid-actuation!

echo "Restoring CPU Governors to Performance..."
for c in {4..29}; do
    echo 'performance' > "/sys/devices/system/cpu/cpu${c}/cpufreq/scaling_governor" 2>/dev/null || true
done
echo "Governors restored."

# Optionally clean up the hint file so it doesn't linger
rm -f /dev/shm/minimd_phase_hints_myrun.bin 2>/dev/null
echo "Shared memory hints cleared."
