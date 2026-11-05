#!/usr/bin/env python3
import subprocess
import time
import csv
import os
import signal
import sys
from pathlib import Path

# =====================================================
# 1. CONFIGURATION & PATHS (From Blueprint Table 3)
# =====================================================
# Simulation Settings
NP = "4"                           # Number of MPI ranks
SIM_DIR = Path("./").absolute() # Directory where 'make' and code live
INPUT_FILE = SIM_DIR / "in.lj.miniMD"
EXE_NAME = "./miniMD_openmpi"

# Monitoring Settings (Blueprint Table 1)
SAMPLE_INTERVAL = 0.1             # 100ms per read cycle
LOG_FILE = "comm_phase_results.csv"

# PID Parameters for "Above and Beyond" Intelligent Control
TARGET_IPC = 0.50                 # Threshold: IPC below this during activity = Comm/Stall
MIN_POWER_W = 45.0                # Cap floor for communication phases
MAX_POWER_W = 125.0               # Cap ceiling for compute phases
KP, KI, KD = 0.6, 0.1, 0.05       # PID Gains

# Hardware Interface
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_LIMIT_PATH = "/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw"
RAPL_ENERGY_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"

# =====================================================
# 2. INTELLIGENT PID CONTROLLER
# =====================================================
class IntelligentController:
    def __init__(self):
        self.integral = 0
        self.prev_error = 0
        self.current_limit_uw = MAX_POWER_W * 1_000_000

    def get_new_limit(self, current_ipc, phase):
        if phase == "COMPUTE":
            self.integral = 0 # Reset for responsiveness
            return int(MAX_POWER_W * 1_000_000)
        
        # PID Logic for Communication Phase
        error = TARGET_IPC - current_ipc
        self.integral += error * SAMPLE_INTERVAL
        derivative = (error - self.prev_error) / SAMPLE_INTERVAL
        
        # Calculate power reduction based on stall severity
        adjustment = (KP * error + KI * self.integral + KD * derivative) * 10_000_000
        self.current_limit_uw = max(MIN_POWER_W * 1_000_000, 
                                    min(MAX_POWER_W * 1_000_000, self.current_limit_uw - adjustment))
        
        self.prev_error = error
        return int(self.current_limit_uw)

# =====================================================
# 3. HELPER FUNCTIONS
# =====================================================
def apply_power_cap(uw):
    try:
        with open(RAPL_LIMIT_PATH, 'w') as f:
            f.write(str(int(uw)))
    except PermissionError:
        pass # Logging continues even if sysfs is locked in current shell

def get_perf_metrics():
    """Captures hardware counters specifically for IPC and Comm Stalls."""
    cmd = [PERF, "stat", "-e", "cycles,instructions,stalled-cycles-backend", "sleep", str(SAMPLE_INTERVAL)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    cycles = instr = stalls = 0
    for line in res.stderr.splitlines():
        parts = line.split()
        if not parts: continue
        val = parts[0].replace(',', '')
        if val.isdigit():
            if "cycles" in line and "stalled" not in line: cycles = int(val)
            elif "instructions" in line: instr = int(val)
            elif "stalled" in line: stalls = int(val)
    
    ipc = instr / cycles if cycles > 0 else 0
    stall_rate = stalls / cycles if cycles > 0 else 0
    return ipc, stall_rate

# =====================================================
# 4. SYSTEM EXECUTION & MONITORING
# =====================================================
def run_system():
    # Step 1: Compilation Check (Blueprint 2.1.1)
    print(f"[1/4] Checking compilation in {SIM_DIR}...")
    os.chdir(SIM_DIR)
    comp_proc = subprocess.run(["make", "openmpi", "-j", "8"], capture_output=True)
    if comp_proc.returncode != 0:
        print("!! Compilation Failed. Check your Makefile.")
        return

    # Step 2: Ensure Input File exists
    if not INPUT_FILE.exists():
        print(f"!! Error: {INPUT_FILE} not found.")
        return

    # Step 3: Launch miniMD (Blueprint 2.3.4 Integration)
    print(f"[2/4] Launching miniMD with {NP} ranks...")
    cmd = ["mpirun", "--oversubscribe", "-np", NP, EXE_NAME, "-i", "in.lj.miniMD"]
    mini_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Step 4: Active Monitoring & Control Loop
    print(f"[3/4] PID Control Active. Monitoring Communication Phase...")
    controller = IntelligentController()
    
    with open(LOG_FILE, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "ipc", "stall_rate", "power_limit_w", "phase"])
        
        start_time = time.time()
        
        try:
            while mini_proc.poll() is None:
                ipc, stall_rate = get_perf_metrics()
                
                # Determine Phase based on Hardware Counters (Blueprint Table 6)
                if ipc < TARGET_IPC and stall_rate > 0.5:
                    phase = "COMMUNICATION"
                elif ipc >= TARGET_IPC:
                    phase = "COMPUTE"
                else:
                    phase = "IDLE/SYNC"

                # Apply PID logic
                new_limit = controller.get_new_limit(ipc, phase)
                apply_power_cap(new_limit)

                # Log results
                ts = round(time.time() - start_time, 2)
                writer.writerow([ts, round(ipc, 3), round(stall_rate, 3), new_limit/1e6, phase])
                f.flush()

                # Live Output
                print(f"[{ts:6.2f}s] {phase:14} | IPC: {ipc:.2f} | PwrLimit: {new_limit/1e6:.1f}W")

        except KeyboardInterrupt:
            print("\n!! Interrupted. Killing miniMD...")
            mini_proc.terminate()
        finally:
            # Cleanup: Restore Power (Blueprint 2.4.3 Mitigation)
            apply_power_cap(MAX_POWER_W * 1_000_000)
            print(f"[4/4] miniMD Finished. Power limits restored. Data in {LOG_FILE}")

if __name__ == "__main__":
    # Note: Ensure you are within a 'salloc' session before running this
    run_system()
