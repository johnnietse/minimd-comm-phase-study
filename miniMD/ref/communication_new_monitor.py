#!/usr/bin/env python3
"""
Communication Phase Monitor for miniMD
Monitors MPI communication phases using IPC, power metrics, and MPI profiling
Based on ELEC49X Group 30 Blueprint specifications
"""

import time
import subprocess
import csv
import re
import os
import signal
from pathlib import Path
from collections import deque
import statistics

# =====================================================
#                    CONFIGURATION
# =====================================================

# Monitoring Configuration
PERF_INTERVAL = 1.0          # perf sampling interval (seconds)
IPC_COMM_THRESHOLD = 0.6     # below → likely communication/sync wait
POWER_ACTIVE_W = 3.0         # above idle → system active
CSV_PATH = "communication_phase_monitor.csv"
HEARTBEAT_SEC = 5

# Communication Detection Parameters
MPI_WAIT_THRESHOLD = 0.30    # >30% time in MPI = communication phase
SYNC_OVERHEAD_THRESHOLD = 0.25  # Synchronization overhead threshold

# Sliding Window for Phase Stability
WINDOW_SIZE = 3              # Require 3 consecutive detections
phase_history = deque(maxlen=WINDOW_SIZE)

# RAPL and Performance Counter Paths
RAPL_PKG = Path("/sys/class/powercap/intel-rapl:0/energy_uj")
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

# =====================================================
#                    STARTUP LOGIC
# =====================================================

print("[startup] Communication Phase Monitor starting...")

rapl_ok = RAPL_PKG.exists()
if rapl_ok:
    print(f"[startup] RAPL found at {RAPL_PKG}")
else:
    print("[startup] RAPL NOT found, power detection disabled")

# Create CSV with enhanced headers
with open(CSV_PATH, "w") as f:
    f.write("timestamp,ipc,cache_refs,cache_misses,energy_W,mpi_wait_pct,sync_overhead,phase,confidence\n")

print(f"[startup] logging to {CSV_PATH}")

# =====================================================
#               PROCESS DETECTION
# =====================================================

def get_minimd_pids():
    """Return PIDs for miniMD processes."""
    try:
        out = subprocess.check_output(["pgrep", "-f", "miniMD"]).decode().strip()
        return [int(pid) for pid in out.split()] if out else []
    except subprocess.CalledProcessError:
        return []

def get_mpi_rank_count():
    """Determine number of MPI ranks."""
    pids = get_minimd_pids()
    return len(pids)

# =====================================================
#                    RAPL POWER
# =====================================================

def read_rapl(prev_energy):
    """Read RAPL power consumption."""
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
#          PERFORMANCE COUNTER READER
# =====================================================

def safe_extract(line):
    """Extract numeric value from perf output."""
    token = line.split()[0].replace(",", "")
    if token.startswith("<") and token.endswith(">"):
        return 0  # <not supported>
    return int(token) if token.isdigit() else 0

def read_perf_metrics():
    """
    Read performance counters including communication-specific metrics.
    Returns: dict with cycles, instructions, cache stats, and stalled cycles
    """
    cmd = [
        PERF, "stat", "-a",
        "-e", "cycles,instructions,cache-references,cache-misses,stalled-cycles-frontend,stalled-cycles-backend",
        "sleep", str(PERF_INTERVAL)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    cycles = instr = cache_refs = cache_misses = stall_front = stall_back = 0

    for line in result.stderr.splitlines():
        if "cycles" in line and "stalled" not in line:
            cycles = safe_extract(line)
        elif "instructions" in line:
            instr = safe_extract(line)
        elif "cache-references" in line:
            cache_refs = safe_extract(line)
        elif "cache-misses" in line:
            cache_misses = safe_extract(line)
        elif "stalled-cycles-frontend" in line:
            stall_front = safe_extract(line)
        elif "stalled-cycles-backend" in line:
            stall_back = safe_extract(line)

    ipc = instr / cycles if cycles > 0 else 0
    cache_miss_rate = cache_misses / cache_refs if cache_refs > 0 else 0
    stall_ratio = (stall_front + stall_back) / cycles if cycles > 0 else 0

    return {
        'ipc': ipc,
        'cache_refs': cache_refs,
        'cache_misses': cache_misses,
        'cache_miss_rate': cache_miss_rate,
        'stall_ratio': stall_ratio
    }

# =====================================================
#          MPI COMMUNICATION ANALYSIS
# =====================================================

def estimate_mpi_wait_time():
    """
    Estimate time spent in MPI operations by analyzing CPU stalls and low IPC.
    
    Communication phases are characterized by:
    - Low IPC (< 0.6) 
    - High cache miss rates
    - High stalled cycles ratio
    
    Returns: Estimated percentage of time in MPI waits
    """
    metrics = read_perf_metrics()
    
    # Communication indicators
    low_ipc_factor = max(0, (IPC_COMM_THRESHOLD - metrics['ipc']) / IPC_COMM_THRESHOLD)
    cache_factor = min(1.0, metrics['cache_miss_rate'] * 2)  # Normalize to [0,1]
    stall_factor = min(1.0, metrics['stall_ratio'] * 1.5)
    
    # Weighted estimate of MPI wait time
    # Higher values = more likely in communication
    mpi_wait_pct = (0.4 * low_ipc_factor + 0.3 * cache_factor + 0.3 * stall_factor)
    
    return mpi_wait_pct, metrics

def calculate_sync_overhead(metrics, num_ranks):
    """
    Calculate synchronization overhead based on performance metrics.
    
    Synchronization manifests as:
    - Uneven CPU utilization across ranks
    - Periods of low activity followed by bursts
    - High stall ratios with low cache activity
    """
    if num_ranks <= 1:
        return 0.0
    
    # Estimate sync overhead from stall patterns
    # More ranks = higher potential for synchronization delays
    rank_factor = min(1.0, num_ranks / 32.0)  # Normalize by typical core count
    
    sync_overhead = metrics['stall_ratio'] * rank_factor * 0.7
    
    return min(1.0, sync_overhead)

# =====================================================
#          PHASE CLASSIFICATION
# =====================================================

def classify_phase(mpi_wait_pct, sync_overhead, ipc, watts, num_ranks):
    """
    Classify computational phase with communication focus.
    
    Communication Phase Criteria (from blueprint):
    - MPI wait time > 30%
    - Synchronization overhead > 25%
    - Low IPC (< 0.6)
    - Power above idle threshold
    
    Returns: (phase_name, confidence_score)
    """
    
    # Communication phase detection
    comm_indicators = 0
    confidence = 0.0
    
    if mpi_wait_pct > MPI_WAIT_THRESHOLD:
        comm_indicators += 1
        confidence += 0.35
    
    if sync_overhead > SYNC_OVERHEAD_THRESHOLD:
        comm_indicators += 1
        confidence += 0.30
    
    if ipc < IPC_COMM_THRESHOLD:
        comm_indicators += 1
        confidence += 0.20
    
    if num_ranks > 1 and watts > POWER_ACTIVE_W:
        comm_indicators += 1
        confidence += 0.15
    
    # Phase determination
    if comm_indicators >= 3:
        phase = "COMMUNICATION"
        confidence = min(1.0, confidence)
    elif ipc >= IPC_COMM_THRESHOLD and watts > POWER_ACTIVE_W + 5:
        phase = "COMPUTE"
        confidence = 0.7
    elif watts < POWER_ACTIVE_W:
        phase = "IDLE"
        confidence = 0.8
    else:
        phase = "MIXED"
        confidence = 0.5
    
    return phase, confidence

def apply_phase_smoothing(current_phase):
    """
    Apply sliding window smoothing to prevent false phase transitions.
    Requires WINDOW_SIZE consecutive detections for phase change.
    """
    phase_history.append(current_phase)
    
    if len(phase_history) < WINDOW_SIZE:
        return current_phase, False  # Not enough history
    
    # Check if all recent phases match
    if len(set(phase_history)) == 1:
        return current_phase, True  # Stable phase
    else:
        # Return most common phase in window
        return max(set(phase_history), key=phase_history.count), False

# =====================================================
#            WAIT FOR MiniMD TO START
# =====================================================

print("[startup] waiting for MiniMD to launch...")

while True:
    pids = get_minimd_pids()
    if pids:
        num_ranks = len(pids)
        print(f"[start] detected {num_ranks} MPI ranks: {pids}")
        break
    time.sleep(1)

print("[run] entering monitoring loop...")

# =====================================================
#               MAIN MONITORING LOOP
# =====================================================

prev_energy = None
last_hb = time.time()
start_time = time.time()
last_phase = None
phase_start_time = {}

while True:
    pids = get_minimd_pids()
    if not pids:
        print("[stop] MiniMD finished.")
        break

    # Get current rank count
    num_ranks = len(pids)
    
    # Read power
    watts, prev_energy = read_rapl(prev_energy)
    
    # Estimate MPI communication activity
    mpi_wait_pct, metrics = estimate_mpi_wait_time()
    
    # Calculate synchronization overhead
    sync_overhead = calculate_sync_overhead(metrics, num_ranks)
    
    # Classify phase
    raw_phase, confidence = classify_phase(
        mpi_wait_pct, 
        sync_overhead, 
        metrics['ipc'], 
        watts, 
        num_ranks
    )
    
    # Apply smoothing
    phase, is_stable = apply_phase_smoothing(raw_phase)
    
    # Track phase transitions
    timestamp = round(time.time() - start_time, 2)
    
    if phase != last_phase:
        if last_phase is not None:
            duration = timestamp - phase_start_time.get(last_phase, timestamp)
            print(f"[transition @ {timestamp:6.2f}s] {last_phase} → {phase} (duration: {duration:.2f}s)")
        phase_start_time[phase] = timestamp
        last_phase = phase
    
    # Write to CSV
    with open(CSV_PATH, "a") as f:
        f.write(f"{timestamp},{metrics['ipc']:.3f},{metrics['cache_refs']},"
                f"{metrics['cache_misses']},{watts:.3f},{mpi_wait_pct:.3f},"
                f"{sync_overhead:.3f},{phase},{confidence:.2f}\n")
    
    # Console output
    stability_marker = "✓" if is_stable else "~"
    print(
        f"[{timestamp:6.2f}s] {stability_marker} "
        f"IPC={metrics['ipc']:.2f} | MPI_wait={mpi_wait_pct:.1%} | "
        f"Sync={sync_overhead:.1%} | Power={watts:.2f}W | "
        f"Phase={phase} (conf={confidence:.2f})"
    )
    
    # Heartbeat with detailed stats
    now = time.time()
    if now - last_hb >= HEARTBEAT_SEC:
        print(
            f"[hb] ranks={num_ranks}, ipc={metrics['ipc']:.2f}, "
            f"cache_miss={metrics['cache_miss_rate']:.1%}, "
            f"mpi_wait={mpi_wait_pct:.1%}, sync={sync_overhead:.1%}, "
            f"watts={watts:.2f}, phase={phase}"
        )
        last_hb = now

print(f"[complete] Monitoring data saved to {CSV_PATH}")

# =====================================================
#          POST-RUN ANALYSIS SUMMARY
# =====================================================

print("\n[analysis] Generating phase summary...")

try:
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    # Calculate phase durations
    phase_times = {}
    total_time = float(data[-1]['timestamp']) if data else 0
    
    for row in data:
        phase = row['phase']
        phase_times[phase] = phase_times.get(phase, 0) + PERF_INTERVAL
    
    print(f"\nTotal monitoring time: {total_time:.2f}s")
    print("\nPhase Distribution:")
    for phase, duration in sorted(phase_times.items()):
        percentage = (duration / total_time * 100) if total_time > 0 else 0
        print(f"  {phase:15s}: {duration:6.2f}s ({percentage:5.1f}%)")
    
    # Communication phase statistics
    comm_samples = [row for row in data if row['phase'] == 'COMMUNICATION']
    if comm_samples:
        avg_mpi_wait = statistics.mean([float(r['mpi_wait_pct']) for r in comm_samples])
        avg_sync = statistics.mean([float(r['sync_overhead']) for r in comm_samples])
        avg_power = statistics.mean([float(r['energy_W']) for r in comm_samples])
        
        print(f"\nCommunication Phase Statistics:")
        print(f"  Avg MPI wait:       {avg_mpi_wait:.1%}")
        print(f"  Avg sync overhead:  {avg_sync:.1%}")
        print(f"  Avg power draw:     {avg_power:.2f}W")
        print(f"  Samples detected:   {len(comm_samples)}")

except Exception as e:
    print(f"[warning] Could not generate summary: {e}")
