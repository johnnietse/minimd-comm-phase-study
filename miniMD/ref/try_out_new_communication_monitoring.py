#!/usr/bin/env python3
"""
Communication Phase Monitor for MiniMD
Focuses on MPI communication patterns, network activity, and synchronization behavior
"""

import time
import subprocess
import csv
from pathlib import Path
import re
import signal
import sys

# =====================================================
#                     CONFIG
# =====================================================

PERF_INTERVAL = 0.5          # Faster sampling for communication detection
CSV_PATH = "communication_phase_monitor.csv"
HEARTBEAT_SEC = 5

# Communication-specific thresholds
# Research shows communication phases have:
# - Low IPC (< 0.5) due to waiting
# - High stall cycles
# - Low cache activity (data in flight, not in cache)
# - Distinct power signature (lower than compute, higher than idle)
IPC_COMM_THRESHOLD = 0.5     # Below this suggests waiting/communication
STALL_RATIO_THRESHOLD = 0.6  # High stall ratio indicates synchronization
POWER_FLOOR_W = 15.0         # Minimum power to distinguish from idle
POWER_CEILING_W = 35.0       # Maximum power (below compute-bound)

RAPL_PKG = Path("/sys/class/powercap/intel-rapl:0/energy_uj")
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

# =====================================================
#            STARTUP LOGIC
# =====================================================

print("[startup] Communication phase monitor starting...")

rapl_ok = RAPL_PKG.exists()
if rapl_ok:
    print(f"[startup] RAPL found at {RAPL_PKG}")
else:
    print("[startup] RAPL NOT found, power detection disabled")

# Create CSV with communication-specific metrics
with open(CSV_PATH, "w") as f:
    f.write("timestamp,ipc,stall_ratio,cache_refs_per_ms,mpi_rank_activity,power_W,phase\n")
print(f"[startup] logging to {CSV_PATH}")

# =====================================================
#               PROCESS DETECTION
# =====================================================

def get_minimd_pids():
    """Return PIDs for miniMD processes."""
    pids = []
    for name in ("miniMD", "miniMD_openmpi"):
        try:
            out = subprocess.check_output(["pgrep", "-f", name]).decode().strip()
            if out:
                pids.extend(int(pid) for pid in out.split())
        except subprocess.CalledProcessError:
            pass
    return pids

# =====================================================
#                    RAPL POWER
# =====================================================

def read_rapl(prev_energy):
    if not rapl_ok:
        return 0.0, prev_energy
    try:
        energy = int(RAPL_PKG.read_text().strip())
    except:
        return 0.0, prev_energy

    if prev_energy is None:
        return 0.0, energy

    delta = max(0, energy - prev_energy)
    watts = delta / (1e6 * PERF_INTERVAL)
    return watts, energy

# =====================================================
#          COMMUNICATION-SPECIFIC PERF COUNTERS
# =====================================================

def safe_extract(line):
    """Extract numeric value from perf stat output."""
    token = line.split()[0].replace(",", "")
    if token.startswith("<") and token.endswith(">"):
        return 0
    return int(token) if token.isdigit() else 0

def read_comm_metrics():
    """
    Read metrics that specifically indicate communication:
    - IPC (low during waiting)
    - Stalled cycles (high during synchronization)
    - Cache references (low when data is in network buffers)
    - Bus transactions (high during MPI transfers)
    """
    cmd = [
        PERF, "stat", "-a",
        "-e", "cycles,instructions,stalled-cycles-frontend,stalled-cycles-backend,cache-references,bus-cycles",
        "sleep", str(PERF_INTERVAL)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    cycles = instr = stall_fe = stall_be = cache_refs = bus_cycles = 0

    for line in result.stderr.splitlines():
        if "cycles" in line and "stalled" not in line and "bus" not in line:
            cycles = safe_extract(line)
        elif "instructions" in line:
            instr = safe_extract(line)
        elif "stalled-cycles-frontend" in line:
            stall_fe = safe_extract(line)
        elif "stalled-cycles-backend" in line:
            stall_be = safe_extract(line)
        elif "cache-references" in line:
            cache_refs = safe_extract(line)
        elif "bus-cycles" in line:
            bus_cycles = safe_extract(line)

    # Calculate communication indicators
    ipc = instr / cycles if cycles > 0 else 0
    stall_ratio = (stall_fe + stall_be) / cycles if cycles > 0 else 0
    cache_refs_per_ms = cache_refs / (PERF_INTERVAL * 1000)
    
    return ipc, stall_ratio, cache_refs_per_ms, bus_cycles

# =====================================================
#          MPI RANK ACTIVITY DETECTION
# =====================================================

def detect_mpi_activity(pids):
    """
    Check if MPI ranks are in communication state by examining:
    - CPU usage per rank (low during MPI waits)
    - Number of active vs waiting ranks
    """
    if not pids:
        return 0.0
    
    active_ranks = 0
    for pid in pids:
        try:
            # Check if process is running vs waiting
            with open(f"/proc/{pid}/stat") as f:
                stat = f.read().split()
                state = stat[2]  # Process state
                # 'R' = running, 'S' = sleeping (waiting for I/O or sync)
                if state == 'R':
                    active_ranks += 1
        except (FileNotFoundError, IndexError):
            continue
    
    # Return fraction of active ranks
    return active_ranks / len(pids) if pids else 0.0

# =====================================================
#            WAIT FOR MINIMD
# =====================================================

print("[startup] waiting for miniMD...")

while not get_minimd_pids():
    time.sleep(1)

print("[run] miniMD detected — monitoring communication phases")

# =====================================================
#               MAIN MONITORING LOOP
# =====================================================

prev_energy = None
last_hb = time.time()
start_time = time.time()

# Phase detection state machine
comm_phase_streak = 0
STREAK_THRESHOLD = 2  # Need 2 consecutive detections to confirm phase

def classify_phase(ipc, stall_ratio, cache_refs_per_ms, mpi_activity, watts):
    """
    Classification logic based on communication characteristics:
    
    COMMUNICATION phase indicators:
    - Low IPC (< 0.5): CPUs waiting, not executing instructions
    - High stall ratio (> 0.6): Processor stalled waiting for data
    - Low cache activity: Data in network buffers, not cache
    - Moderate power: Lower than compute, higher than idle
    - Low MPI rank activity: Processes waiting in MPI calls
    
    This is fundamentally different from:
    - COMPUTE: High IPC, low stalls, high cache activity
    - MEMORY_BOUND: Moderate IPC, high cache misses, high memory bandwidth
    - IDLE: Very low power, no activity
    """
    global comm_phase_streak
    
    # Communication detection
    is_communication = (
        ipc < IPC_COMM_THRESHOLD and
        stall_ratio > STALL_RATIO_THRESHOLD and
        watts > POWER_FLOOR_W and
        watts < POWER_CEILING_W and
        mpi_activity < 0.4  # Less than 40% of ranks actively computing
    )
    
    if is_communication:
        comm_phase_streak += 1
        if comm_phase_streak >= STREAK_THRESHOLD:
            return "COMMUNICATION"
    else:
        comm_phase_streak = 0
    
    # Fallback classifications
    if watts < POWER_FLOOR_W:
        return "IDLE"
    elif ipc > 1.0 and mpi_activity > 0.7:
        return "COMPUTE"
    elif stall_ratio > 0.7:  # Different pattern than communication
        return "MEMORY_BOUND"
    else:
        return "MIXED"

try:
    while True:
        pids = get_minimd_pids()
        if not pids:
            print("[stop] miniMD finished.")
            break

        # Read communication-specific metrics
        ipc, stall_ratio, cache_refs_per_ms, bus_cycles = read_comm_metrics()
        
        # Check MPI rank activity
        mpi_activity = detect_mpi_activity(pids)
        
        # Read power
        watts, prev_energy = read_rapl(prev_energy)

        # Classify phase
        phase = classify_phase(ipc, stall_ratio, cache_refs_per_ms, mpi_activity, watts)

        # Log data
        timestamp = round(time.time() - start_time, 2)
        with open(CSV_PATH, "a") as f:
            f.write(f"{timestamp},{ipc:.3f},{stall_ratio:.3f},{cache_refs_per_ms:.1f},"
                   f"{mpi_activity:.3f},{watts:.2f},{phase}\n")

        # Console output
        print(
            f"[{timestamp:5.1f}s] "
            f"IPC={ipc:.2f} | Stall={stall_ratio:.2f} | "
            f"CacheRefs={cache_refs_per_ms:.0f}/ms | "
            f"MPIact={mpi_activity:.2f} | "
            f"Power={watts:.2f}W | Phase={phase}"
        )

        # Heartbeat
        if time.time() - last_hb >= HEARTBEAT_SEC:
            comm_pct = 100.0 * comm_phase_streak / max(1, timestamp / PERF_INTERVAL)
            print(
                f"[hb] ranks={len(pids)}, phase={phase}, "
                f"comm_detected={comm_pct:.1f}% of time"
            )
            last_hb = time.time()

except KeyboardInterrupt:
    print("\n[stop] Monitor interrupted by user")
finally:
    print(f"[complete] Data logged to {CSV_PATH}")
