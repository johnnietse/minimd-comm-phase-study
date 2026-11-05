#!/usr/bin/env python3
"""
Communication Phase Monitor & Control for miniMD
Monitors MPI communication phases using IPC, power metrics, and MPI profiling.
Implements dynamic power control (DVFS/RAPL) to save energy during communication.
Based on ELEC49X Group 30 Blueprint specifications
"""

import time
import subprocess
import csv
import re
import os
import signal
import sys
from pathlib import Path
from collections import deque
import statistics

# =====================================================
#                    CONFIGURATION
# =====================================================

# Monitoring Configuration
PERF_INTERVAL = 0.1          # 100ms sampling interval (Target: <1 ms latency)
IPC_COMM_THRESHOLD = 0.6     # below → likely communication/sync wait
POWER_ACTIVE_W = 3.0         # above idle → system active
CSV_PATH = "communication_phase_monitor.csv"
HEARTBEAT_SEC = 5

# Communication Detection Parameters
MPI_WAIT_THRESHOLD = 0.30    # >30% time in MPI = communication phase
SYNC_OVERHEAD_THRESHOLD = 0.25  # Synchronization overhead threshold

# Control Parameters
COMMUNICATION_FREQ_KHZ = 1200000  # Target frequency during communication (1.2 GHz)
NORMAL_FREQ_KHZ = 2400000         # Normal frequency (2.4 GHz) - EXAMPLE, adjust to HW
COMMUNICATION_POWER_W = 60        # Power limit during communication
NORMAL_POWER_W = 120              # Normal TDP

# Sliding Window for Phase Stability
WINDOW_SIZE = 3              # Require 3 consecutive detections
phase_history = deque(maxlen=WINDOW_SIZE)

# RAPL and Performance Counter Paths
RAPL_BASE = Path("/sys/class/powercap/intel-rapl")
RAPL_PKG = RAPL_BASE / "intel-rapl:0" / "energy_uj"
RAPL_LIMIT = RAPL_BASE / "intel-rapl:0" / "constraint_0_power_limit_uw"
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

# =====================================================
#               Exectution Module (Actuation)
# =====================================================

class ExecutionModule:
    """Handles system-level changes to frequency and power."""
    
    def __init__(self):
        self.rapl_enabled = RAPL_PKG.exists()
        self.cpupower_available = self._check_cpupower()
        self.current_freq = None
        self.current_power = None
    
    def _check_cpupower(self):
        """Check if cpupower is available."""
        try:
            subprocess.run(["cpupower", "--version"], capture_output=True, check=True)
            return True
        except:
            return False

    def set_cpu_frequency(self, freq_khz):
        """Set CPU frequency using cpupower."""
        if not self.cpupower_available:
            return False
            
        if self.current_freq == freq_khz:
            return True # Already set

        try:
            # Set max frequency for all cores
            cmd = ["cpupower", "frequency-set", "-u", str(freq_khz) + "kHz"]
            subprocess.run(cmd, capture_output=True, check=True)
            self.current_freq = freq_khz
            return True
        except subprocess.CalledProcessError:
            return False

    def set_rapl_limit(self, watts):
        """Set RAPL power limit."""
        if not self.rapl_enabled or not RAPL_LIMIT.exists():
            return False
            
        if self.current_power == watts:
            return True

        microwatts = int(watts * 1e6)
        try:
            # Requires root/sudo, script assumes permissions or sudo execution
            # Try writing directly if we have permission
            try:
                RAPL_LIMIT.write_text(str(microwatts))
            except PermissionError:
                # Try via sudo tee
                cmd = f"echo {microwatts} | sudo tee {RAPL_LIMIT}"
                subprocess.run(cmd, shell=True, check=True, capture_output=True)
            
            self.current_power = watts
            return True
        except Exception:
            return False

# =====================================================
#             Control Module (Logic)
# =====================================================

class ControlModule:
    """Decides on actions based on phase."""
    
    def __init__(self, execution_module):
        self.exec = execution_module
        self.active_policy = "NORMAL"

    def apply_policy(self, phase):
        """Apply power policy based on phase."""
        target_policy = "NORMAL"
        
        if phase == "COMMUNICATION":
            target_policy = "COMM_SAVE"
        elif phase == "IDLE":
            target_policy = "IDLE" # Could be same as COMM_SAVE
        else:
            target_policy = "NORMAL"

        if target_policy == self.active_policy:
            return # No change needed

        print(f"[control] Switching policy: {self.active_policy} -> {target_policy}")

        if target_policy == "COMM_SAVE":
            # Reduce frequency and power
            self.exec.set_cpu_frequency(COMMUNICATION_FREQ_KHZ)
            # self.exec.set_rapl_limit(COMMUNICATION_POWER_W) # Optional: Use if RAPL preferred
        elif target_policy == "NORMAL":
            # Restore max performance
            self.exec.set_cpu_frequency(NORMAL_FREQ_KHZ)
            # self.exec.set_rapl_limit(NORMAL_POWER_W)
        
        self.active_policy = target_policy


# =====================================================
#                    STARTUP LOGIC
# =====================================================

print("[startup] Communication Phase Monitor & Control starting...")

execution_mod = ExecutionModule()
control_mod = ControlModule(execution_mod)

if execution_mod.rapl_enabled:
    print(f"[startup] RAPL found at {RAPL_PKG}")
else:
    print("[startup] RAPL NOT found, power detection disabled (Simulation Mode)")

if execution_mod.cpupower_available:
    print("[startup] cpupower available for DVFS")
else:
    print("[startup] cpupower NOT found, frequency scaling disabled (Simulation Mode)")

# Create CSV with enhanced headers INCLUDING UNITS
with open(CSV_PATH, "w") as f:
    f.write("timestamp(s),ipc,cache_refs,cache_misses,energy(J),power(W),mpi_wait_pct(%),sync_overhead(%),phase,confidence,policy\n")

print(f"[startup] logging to {CSV_PATH}")

# =====================================================
#               PROCESS DETECTION
# =====================================================

def get_minimd_pids():
    """Return PIDs for miniMD processes."""
    # Try pgrep (Linux/HPC)
    try:
        out = subprocess.check_output(["pgrep", "-f", "miniMD"]).decode().strip()
        return [int(pid) for pid in out.split()] if out else []
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback for Windows or missing pgrep
        try:
             # Very basic Windows check (tasklist)
             cmd = 'tasklist /FI "IMAGENAME eq miniMD.exe" /NH'
             out = subprocess.check_output(cmd, shell=True).decode()
             if "No tasks are running" in out:
                 return []
             # Parsing tasklist output is messy, just returning empty to trigger demo mode for now
             # unless we really need to track a windows process.
             return []
        except:
             return []

# =====================================================
#                    RAPL POWER
# =====================================================

def read_rapl(prev_energy):
    """Read RAPL power consumption."""
    if not execution_mod.rapl_enabled:
        return 0.0, 0.0, prev_energy

    try:
        energy = int(RAPL_PKG.read_text().strip())
    except:
        return 0.0, 0.0, prev_energy

    if prev_energy is None:
        return 0.0, 0.0, energy

    delta = max(0, energy - prev_energy)
    watts = delta / (1e6 * PERF_INTERVAL)
    # Energy in Joules is just delta / 1e6
    energy_joules = delta / 1e6
   
    return watts, energy_joules, energy

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
    # Check if perf exists, else mock
    if not os.path.exists(PERF):
         # Mocking for local testing if perf not found (e.g., Windows/Non-HPC)
         return {
            'ipc': 1.0,
            'cache_refs': 1000,
            'cache_misses': 100,
            'cache_miss_rate': 0.1,
            'stall_ratio': 0.2
        }

    try:
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
    except Exception:
         return {
            'ipc': 0, 'cache_refs': 0, 'cache_misses': 0, 
            'cache_miss_rate': 0, 'stall_ratio': 0
        }

# =====================================================
#          MPI COMMUNICATION ANALYSIS
# =====================================================

def estimate_mpi_wait_time():
    """Estimate time spent in MPI operations."""
    metrics = read_perf_metrics()
    
    # Communication indicators
    low_ipc_factor = max(0, (IPC_COMM_THRESHOLD - metrics['ipc']) / IPC_COMM_THRESHOLD)
    cache_factor = min(1.0, metrics['cache_miss_rate'] * 2)
    stall_factor = min(1.0, metrics['stall_ratio'] * 1.5)
    
    mpi_wait_pct = (0.4 * low_ipc_factor + 0.3 * cache_factor + 0.3 * stall_factor)
    
    return mpi_wait_pct, metrics

def calculate_sync_overhead(metrics, num_ranks):
    """Calculate synchronization overhead."""
    if num_ranks <= 1:
        return 0.0
    
    rank_factor = min(1.0, num_ranks / 32.0)
    sync_overhead = metrics['stall_ratio'] * rank_factor * 0.7
    
    return min(1.0, sync_overhead)

# =====================================================
#          PHASE CLASSIFICATION
# =====================================================

def classify_phase(mpi_wait_pct, sync_overhead, ipc, watts, num_ranks):
    """Classify computational phase."""
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
    """Apply sliding window smoothing."""
    phase_history.append(current_phase)
    
    if len(phase_history) < WINDOW_SIZE:
        return current_phase, False
    
    if len(set(phase_history)) == 1:
        return current_phase, True
    else:
        return max(set(phase_history), key=phase_history.count), False

# =====================================================
#            WAIT FOR MiniMD TO START
# =====================================================

print("[startup] waiting for MiniMD to launch...")

# Use a timeout or manual check for local testing
wait_start = time.time()
while True:
    pids = get_minimd_pids()
    if pids:
        num_ranks = len(pids)
        print(f"[start] detected {num_ranks} MPI ranks: {pids}")
        break
    
    # For local Testing without MiniMD, break after 5s to allow script to run partially
    if not pids and (time.time() - wait_start > 5):
        print("[start] MiniMD not detected, starting in DEMO/TEST mode...")
        # Demo Values
        pids = [1234]
        num_ranks = 4
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

try:
    while True:
        # Check if process is still alive (skip in demo mode)
        current_pids = get_minimd_pids()
        if not current_pids and num_ranks != 4: # rough check for demo mode
            print("[stop] MiniMD finished.")
            break

        # Read power
        if execution_mod.rapl_enabled:
             watts, energy_joules, prev_energy = read_rapl(prev_energy)
        else:
             # Simulation data
             watts = 100.0
             energy_joules = 10.0
             prev_energy = (prev_energy or 0) + 1000000 

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
        
        # Apply Control Policy
        if is_stable:
            control_mod.apply_policy(phase)

        # Track phase transitions
        timestamp = round(time.time() - start_time, 2)
        
        if phase != last_phase:
            if last_phase is not None:
                duration = timestamp - phase_start_time.get(last_phase, timestamp)
                print(f"[transition @ {timestamp:6.2f}s] {last_phase} -> {phase} (duration: {duration:.2f}s)")
            phase_start_time[phase] = timestamp
            last_phase = phase
        
        # Write to CSV
        with open(CSV_PATH, "a") as f:
            f.write(f"{timestamp},{metrics['ipc']:.3f},{metrics['cache_refs']},"
                    f"{metrics['cache_misses']},{energy_joules:.3f},{watts:.3f},{mpi_wait_pct:.3f},"
                    f"{sync_overhead:.3f},{phase},{confidence:.2f},{control_mod.active_policy}\n")
        
        # Console output
        stability_marker = "✓" if is_stable else "~"
        print(
            f"[{timestamp:6.2f}s] {stability_marker} "
            f"IPC={metrics['ipc']:.2f} | MPI_wait={mpi_wait_pct:.1%} | "
            f"Sync={sync_overhead:.1%} | Power={watts:.2f}W | "
            f"Phase={phase} | Policy={control_mod.active_policy}"
        )
        
        # Heartbeat
        now = time.time()
        if now - last_hb >= HEARTBEAT_SEC:
            print(f"[hb] ranks={num_ranks}, watts={watts:.2f}, phase={phase}")
            last_hb = now
        
        # Enforce sampling interval (perf sleep usually handles this, but careful with overhead)
        # If PERF command is used, it sleeps. If mocked, we must sleep.
        if not os.path.exists(PERF):
            time.sleep(PERF_INTERVAL)


except KeyboardInterrupt:
    print("\n[stop] Interrupted by user.")

print(f"[complete] Monitoring data saved to {CSV_PATH}")

# =====================================================
#          POST-RUN ANALYSIS SUMMARY
# =====================================================

print("\n[analysis] Generating phase summary...")

try:
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    total_time = float(data[-1]['timestamp(s)']) if data else 0
    phase_times = {}
    
    # Simple count-based duration
    for row in data:
        phase = row['phase']
        phase_times[phase] = phase_times.get(phase, 0) + PERF_INTERVAL
    
    print(f"\nTotal monitoring time: {total_time:.2f}s")
    print("\nPhase Distribution:")
    for phase, duration in sorted(phase_times.items()):
        percentage = (duration / total_time * 100) if total_time > 0 else 0
        print(f"  {phase:15s}: {duration:6.2f}s ({percentage:5.1f}%)")
    
    comm_samples = [row for row in data if row['phase'] == 'COMMUNICATION']
    if comm_samples:
        avg_mpi_wait = statistics.mean([float(r['mpi_wait_pct(%)']) for r in comm_samples])
        avg_power = statistics.mean([float(r['power(W)']) for r in comm_samples])
        
        print(f"\nCommunication Phase Statistics:")
        print(f"  Avg MPI wait:       {avg_mpi_wait:.1%}")
        print(f"  Avg power draw:     {avg_power:.2f}W")

except Exception as e:
    print(f"[warning] Could not generate summary: {e}")
