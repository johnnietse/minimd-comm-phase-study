#!/usr/bin/env python3
import time
import subprocess
import csv
import os
import signal
from pathlib import Path

# =====================================================
# 1. ADVANCED CONFIGURATION (Aligned with Blueprint Table 1 & 6)
# =====================================================
SAMPLE_INTERVAL = 0.1  # 100ms (Requirement: Monitoring Interval)
CSV_LOG = "intelligent_comm_control.csv"

# Thresholds for Phase Detection
IPC_COMM_THRESHOLD = 0.45    # Low IPC during high cycles indicates MPI Wait
STALL_RATIO_THRESHOLD = 0.65 # Ratio of stalled cycles
POWER_IDLE_BASELINE = 25.0   # Baseline Watts for your CAC node (AMD EPYC)

# PID Constants for Power Capping
# Purpose: Smoothly adjust power limits to prevent "Control Instability" (Blueprint 2.4.2.4)
KP = 0.5   # Proportional: Instant reaction to IPC drops
KI = 0.1   # Integral: Adjusts based on sustained comm phases
KD = 0.05  # Derivative: Prevents overshooting/oscillation

# Hardware Paths
RAPL_LIMIT_PATH = "/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw"
RAPL_ENERGY_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
PERF_EXE = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

# =====================================================
# 2. PID CONTROLLER CLASS
# =====================================================
class PowerPID:
    def __init__(self, target_ipc, min_watts, max_watts):
        self.target_ipc = target_ipc
        self.min_limit = min_watts * 1_000_000 # Convert to microwatts
        self.max_limit = max_watts * 1_000_000
        self.prev_error = 0
        self.integral = 0
        self.current_limit = self.max_limit

    def compute(self, current_ipc):
        # Error is the difference between current efficiency and target
        error = self.target_ipc - current_ipc
        self.integral += error * SAMPLE_INTERVAL
        derivative = (error - self.prev_error) / SAMPLE_INTERVAL
        
        # Output adjustment
        output = (KP * error) + (KI * self.integral) + (KD * derivative)
        
        # Adjust power limit: If IPC is low (Comm phase), reduce power
        # Note: Logic is inverted because lower IPC -> lower power limit
        adjustment = output * 10_000_000 # Scaling factor
        self.current_limit = max(self.min_limit, min(self.max_limit, self.current_limit - adjustment))
        
        self.prev_error = error
        return int(self.current_limit)

# =====================================================
# 3. CORE UTILITIES
# =====================================================
def set_power_limit(uw):
    """Writes the power limit to the Linux Kernel sysfs."""
    try:
        # Note: Requires appropriate permissions on CAC node or 'sudo'
        with open(RAPL_LIMIT_PATH, 'w') as f:
            f.write(str(uw))
    except PermissionError:
        pass # In a real test, this would be the primary actuation

def get_perf_metrics():
    """Captures hardware counters using perf."""
    cmd = [
        PERF_EXE, "stat", "-e", 
        "cycles,instructions,stalled-cycles-backend,stalled-cycles-frontend",
        "sleep", str(SAMPLE_INTERVAL)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    
    data = {"cycles": 0, "instr": 0, "stalls": 0}
    for line in res.stderr.splitlines():
        parts = line.split()
        if not parts: continue
        val = parts[0].replace(',', '')
        if val.isdigit():
            if "cycles" in line and "stalled" not in line: data["cycles"] = int(val)
            if "instructions" in line: data["instr"] = int(val)
            if "stalled" in line: data["stalls"] += int(val)
    
    ipc = data["instr"] / data["cycles"] if data["cycles"] > 0 else 0
    stall_rate = data["stalls"] / data["cycles"] if data["cycles"] > 0 else 0
    return ipc, stall_rate

def read_energy():
    return int(Path(RAPL_ENERGY_PATH).read_text().strip())

# =====================================================
# 4. MAIN INTELLIGENT MONITOR & CONTROL
# =====================================================
def main():
    print(f"--- [PHASE: COMMUNICATION OPTIMIZER] ---")
    print(f"Targeting: 10-15% Energy Savings | Max 3% Perf Loss")
    
    # Initialize PID: Target IPC is 1.0 (Compute efficiency)
    # Range: 40W (comm) to 120W (max compute)
    pid = PowerPID(target_ipc=0.8, min_watts=45, max_watts=125)
    
    # Setup CSV
    with open(CSV_LOG, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "ipc", "stall_rate", "power_w", "limit_w", "phase"])

        start_time = time.time()
        prev_energy = read_energy()
        
        try:
            while True:
                # 1. Monitoring Phase
                ipc, stall_rate = get_perf_metrics()
                curr_energy = read_energy()
                watts = (curr_energy - prev_energy) / (SAMPLE_INTERVAL * 1_000_000)
                prev_energy = curr_energy
                
                # 2. Decision Logic (Intelligence)
                # If IPC is very low and Stall rate is high, we are definitely in MPI Communication
                if stall_rate > STALL_RATIO_THRESHOLD and ipc < IPC_COMM_THRESHOLD:
                    phase = "COMMUNICATION"
                    # Active PID Control: Squeeze power limit
                    new_limit_uw = pid.compute(ipc)
                elif ipc > 0.1:
                    phase = "COMPUTE"
                    # Quickly restore power for compute phases
                    new_limit_uw = 125_000_000 
                    pid.integral = 0 # Reset integral to prevent lag in compute
                else:
                    phase = "IDLE"
                    new_limit_uw = 30_000_000

                # 3. Actuation Phase (Execution Module)
                set_power_limit(new_limit_uw)
                
                # 4. Logging
                ts = round(time.time() - start_time, 2)
                writer.writerow([ts, round(ipc, 3), round(stall_rate, 3), 
                                 round(watts, 2), new_limit_uw/1e6, phase])
                f.flush()

                print(f"[{ts:6.2f}s] {phase:14} | IPC: {ipc:.2f} | Stalls: {stall_rate:.2f} | "
                      f"Power: {watts:6.2f}W | SetLimit: {new_limit_uw/1e6:.1f}W")

                # Heartbeat/Stability Check
                if watts > 150: # Safety Guardrail (Blueprint 2.4.2.4)
                    print("!! [ALERT] Control Instability detected. Reverting to Baseline.")
                    set_power_limit(125_000_000)

        except KeyboardInterrupt:
            print("\n[Shutting Down] Reverting hardware to default state...")
            set_power_limit(125_000_000)

if __name__ == "__main__":
    main()
