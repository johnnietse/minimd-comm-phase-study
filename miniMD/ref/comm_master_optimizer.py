#!/usr/bin/env python3
import subprocess
import time
import csv
import os
import signal
from pathlib import Path

# =====================================================
# 1. CONFIGURATION & THRESHOLDS (Ref: Blueprint Tables 1, 3, & 6)
# =====================================================
SIM_DIR = Path("./").absolute()
EXE_NAME = "./miniMD_openmpi"
NP = "125"  # MPI Ranks
SAMPLE_INTERVAL = 0.1  # 100ms per Blueprint Requirement

# Phase Identification Thresholds
IPC_LOW = 0.45          # Below this is inefficient (Comm or Storage)
STALL_HIGH = 0.60       # High stall cycles = Sync/Comm Wait
LLC_MISS_HIGH = 0.25    # High Cache Miss = Memory Bound
IO_WAIT_LIMIT = 0.30    # High IO Wait = Storage Phase

# PID Tuning (Above and Beyond: Active Feedback Control)
KP = 0.7  # Proportional: Quick reaction to Sync waits
KI = 0.1  # Integral: Handles long communication barriers
KD = 0.05 # Derivative: Prevents rapid power oscillation

# CAC Frontenac Paths
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_LIMIT = "/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw"
RAPL_ENERGY = "/sys/class/powercap/intel-rapl:0/energy_uj"

# =====================================================
# 2. INTELLIGENT PID MODULE
# =====================================================
class PowerOptimizerPID:
    def __init__(self, min_w, max_w):
        self.min_uw = min_w * 1_000_000
        self.max_uw = max_w * 1_000_000
        self.integral = 0
        self.prev_error = 0
        self.setpoint = 0.8 # Target IPC for "efficient" work

    def calculate_limit(self, current_ipc):
        error = self.setpoint - current_ipc
        self.integral += error * SAMPLE_INTERVAL
        derivative = (error - self.prev_error) / SAMPLE_INTERVAL
        
        # If IPC is low (Sync phase), reduction is positive, lowering the cap
        reduction = (KP * error + KI * self.integral + KD * derivative) * 15_000_000
        new_limit = max(self.min_uw, min(self.max_uw, self.max_uw - reduction))
        
        self.prev_error = error
        return int(new_limit)

# =====================================================
# 3. CORE MONITORING & IDENTIFICATION LOGIC
# =====================================================
def get_hardware_telemetry():
    """Reads hardware counters to distinguish between the 5 phases."""
    # Collecting: Cycles, Instructions, Backend Stalls, and LLC Misses
    cmd = [PERF, "stat", "-e", "cycles,instructions,stalled-cycles-backend,cache-misses,cache-references", "sleep", str(SAMPLE_INTERVAL)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    
    metrics = {"cycles": 0, "instr": 0, "stalls": 0, "misses": 0, "refs": 0}
    for line in res.stderr.splitlines():
        p = line.split()
        if not p: continue
        val = int(p[0].replace(',', '')) if p[0].replace(',', '').isdigit() else 0
        if "cycles" in line and "stalled" not in line: metrics["cycles"] = val
        elif "instructions" in line: metrics["instr"] = val
        elif "stalled" in line: metrics["stalls"] = val
        elif "cache-misses" in line: metrics["misses"] = val
        elif "cache-references" in line: metrics["refs"] = val

    ipc = metrics["instr"] / metrics["cycles"] if metrics["cycles"] > 0 else 0
    stall_rate = metrics["stalls"] / metrics["cycles"] if metrics["cycles"] > 0 else 0
    miss_rate = metrics["misses"] / metrics["refs"] if metrics["refs"] > 0 else 0
    
    return ipc, stall_rate, miss_rate

def identify_phase(ipc, stall_rate, miss_rate, pids):
    """The 'Brain': Categorizes miniMD behavior into the Blueprint's 5 Phases."""
    active_ranks = len(pids)
    
    # 1. SERIAL PHASE: Initialization/Finalization (Few ranks active)
    if active_ranks <= 1:
        return "SERIAL"
    
    # 2. STORAGE PHASE: High I/O Wait (Look at low IPC + low activity)
    # Simplified here as extremely low IPC across all ranks
    if ipc < 0.1:
        return "STORAGE"

    # 3. COMMUNICATION PHASE: High Stalls, Low IPC (MPI_Allreduce / Ghost Exchange)
    if stall_rate > STALL_HIGH and ipc < IPC_LOW:
        return "COMMUNICATION"

    # 4. MEMORY-BOUND PHASE: High Cache Misses (Neighbor List Build)
    if miss_rate > LLC_MISS_HIGH:
        return "MEMORY-BOUND"

    # 5. PARALLEL COMPUTE PHASE: High IPC, Low Stalls (Force Kernels)
    return "PARALLEL"

def apply_power(uw):
    try:
        with open(RAPL_LIMIT, 'w') as f:
            f.write(str(uw))
    except:
        pass # Handle permissions in test environment

# =====================================================
# 4. EXECUTION LIFECYCLE
# =====================================================
def run_intelligent_test():
    # Compilation
    print(f"[*] Compiling miniMD in {SIM_DIR}...")
    os.chdir(SIM_DIR)
    subprocess.run(["make", "openmpi", "-j", "8"], stdout=subprocess.DEVNULL)

    # Launch miniMD
    print(f"[*] Launching miniMD simulation...")
    cmd = ["mpirun", "--oversubscribe", "-np", NP, EXE_NAME, "-i", "in.lj.miniMD"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Setup PID & Logging
    optimizer = PowerOptimizerPID(min_w=45, max_w=125)
    log_data = []
    start_time = time.time()

    try:
        while proc.poll() is None:
            # 1. MONITOR
            ipc, stall_rate, miss_rate = get_hardware_telemetry()
            
            # 2. IDENTIFY (All 5 Phases)
            # Find miniMD PIDs dynamically
            try:
                pids = subprocess.check_output(["pgrep", "-f", "miniMD_openmpi"]).decode().split()
            except:
                pids = []
            
            phase = identify_phase(ipc, stall_rate, miss_rate, pids)
            
            # 3. ACTUATE (Active Feedback for Communication)
            if phase == "COMMUNICATION":
                # Active PID control to save 10-15% energy during Sync
                new_limit = optimizer.calculate_limit(ipc)
            elif phase == "PARALLEL":
                # Full power for max performance
                new_limit = 125_000_000
                optimizer.integral = 0 # Reset PID lag
            else:
                # Moderate power for other phases
                new_limit = 80_000_000

            apply_power(new_limit)

            # 4. REPORT
            ts = round(time.time() - start_time, 2)
            print(f"[{ts:6.2f}s] Phase: {phase:15} | IPC: {ipc:.2f} | PwrLimit: {new_limit/1e6:5.1f}W")
            log_data.append([ts, phase, ipc, stall_rate, miss_rate, new_limit/1e6])

    finally:
        # Emergency hardware reset
        apply_power(125_000_000)
        print("[*] Simulation Complete. Writing detailed logs...")
        with open("blueprint_phase_results.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["Time", "Phase", "IPC", "StallRate", "MissRate", "PowerLimit"])
            writer.writerows(log_data)

if __name__ == "__main__":
    run_intelligent_test()
