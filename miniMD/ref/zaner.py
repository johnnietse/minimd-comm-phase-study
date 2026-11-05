#!/usr/bin/env python3
import os
import time
import csv
import subprocess
import fcntl
import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from typing import List, Dict

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PERF_BIN = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
if not os.path.exists(PERF_BIN):
    PERF_BIN = "perf"

APP_PGREP_PATTERN = "miniMD_openmpi"
WCHAR_THRESHOLD_BPS = 0.5 * 1024 * 1024  
IGNORE_IO_FIRST_N_SEC = 5.0
MIN_CHECKPOINT_DURATION_SEC = 30.0
CHECKPOINT_IO_TIMEOUT_SEC = 2.0

# Fixed Frequencies
FREQ_MAX = 2000000 
FREQ_MID = 1600000
FREQ_MIN = 1200000

# Logic Thresholds
MPKI_MEM_THRESHOLD = 10.0      

@dataclass 
class RankMetrics:
    pid: int
    core_id: int
    ipc: float
    mpki: float
    phase: str

# ==============================================================================
# SECTION 1: PER-PROCESS PERF STREAMING
# ==============================================================================

class SingleProcessPerf:
    def __init__(self, pid):
        self.pid = pid
        self.process = None
        self.last_ipc = 0.0
        self.last_mpki = 0.0
        self._start()

    def _start(self):
        cmd = [PERF_BIN, "stat", "-I", "200", 
               "-e", "cycles:u,instructions:u,cache-misses:u", 
               "-p", str(self.pid), "-x", ","]
        try:
            self.process = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
            fd = self.process.stderr.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        except:
            self.process = None

    def read(self):
        if not self.process: return self.last_ipc, self.last_mpki
        
        cycles = instr = misses = 0
        found_data = False

        try:
            while True:
                line = self.process.stderr.readline()
                if not line: break
                parts = line.strip().split(',')
                if len(parts) < 3: continue
                try:
                    val = int(float(parts[1]))
                    event = parts[3]
                    if "cycles" in event: cycles = val
                    elif "instructions" in event: instr = val
                    elif "misses" in event: misses = val
                    found_data = True
                except: continue
        except: pass

        if found_data and cycles > 0:
            self.last_ipc = instr / cycles
            self.last_mpki = (misses / instr * 1000) if instr > 0 else 0.0
            
        return self.last_ipc, self.last_mpki

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.wait()

class PerfManager:
    def __init__(self):
        self.monitors = {} 

    def update(self, current_pids):
        # Remove dead
        for pid in list(self.monitors.keys()):
            if pid not in current_pids:
                self.monitors[pid].stop()
                del self.monitors[pid]
        # Add new
        for pid in current_pids:
            if pid not in self.monitors:
                self.monitors[pid] = SingleProcessPerf(pid)

    def sample_all(self):
        results = {}
        for pid, mon in self.monitors.items():
            ipc, mpki = mon.read()
            results[pid] = (ipc, mpki)
        return results

# ==============================================================================
# SECTION 2: METRICS & IO
# ==============================================================================

class IODetector:
    def __init__(self):
        self.last_wchar: Dict[int, int] = {}
        self.t0 = time.time()
        self.in_checkpoint = False
        self.checkpoint_start_time = None
        self.last_io_time = None

    def get_pids(self) -> List[int]:
        try:
            # Use -x to match exact binary name (ignores mpirun)
            out = subprocess.check_output(
                ["pgrep", "-u", os.environ.get("USER"), "-x", "miniMD_openmpi"],
                text=True
            ).strip()
            if not out: return []
            return sorted({int(x) for x in out.split() if x.isdigit()})
        except: return []

    def check_io(self, pids, dt):
        now = time.time()
        total_dwchar = 0
        
        for pid in pids:
            try:
                with open(f"/proc/{pid}/io", "r") as f:
                    val = 0
                    for line in f:
                        if line.startswith("wchar:"):
                            val = int(line.split()[1])
                            break
                    prev = self.last_wchar.get(pid, val)
                    if val >= prev: total_dwchar += (val - prev)
                    self.last_wchar[pid] = val
            except: pass

        bw = total_dwchar / max(dt, 1e-6)
        t = now - self.t0
        io_detected = (bw >= WCHAR_THRESHOLD_BPS) and (t >= IGNORE_IO_FIRST_N_SEC)

        if io_detected and not self.in_checkpoint:
            self.in_checkpoint = True
            self.checkpoint_start_time = now
            self.last_io_time = now
        elif self.in_checkpoint:
            if io_detected: self.last_io_time = now
            t_in = now - self.checkpoint_start_time
            t_last = now - self.last_io_time if self.last_io_time else 0
            if t_in >= MIN_CHECKPOINT_DURATION_SEC and t_last >= CHECKPOINT_IO_TIMEOUT_SEC:
                self.in_checkpoint = False
                
        return self.in_checkpoint

# ==============================================================================
# SECTION 3: CONTROLLER
# ==============================================================================

class DirectFrequencyController:
    def __init__(self, allowed_cores: List[int]):
        self.handles = {}
        for c in allowed_cores:
            try:
                self.handles[c] = open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed", 'w')
            except: pass

    def set_freq(self, core, freq):
        if core in self.handles:
            try:
                self.handles[core].seek(0)
                self.handles[core].write(str(freq))
                self.handles[core].flush()
            except: pass

    def close(self):
        for h in self.handles.values(): h.close()

# ==============================================================================
# SECTION 4: MAIN MONITOR
# ==============================================================================

class NativeMonitor:
    def __init__(self, cores):
        self.controller = DirectFrequencyController(cores)
        self.io_detector = IODetector()
        self.perf_manager = PerfManager()
        
        self.rapl_file = open("/sys/class/powercap/intel-rapl:0/energy_uj", 'r')
        self.last_energy = int(self.rapl_file.read())
        self.last_time = time.time()
        self.total_joules = 0.0  # <--- NEW: Internal Accumulator
        
        self.csv = open("monitor_log.csv", 'w', newline='')
        self.writer = csv.writer(self.csv)
        self.writer.writerow(['timestamp', 'global_phase', 'avg_ipc', 'pwr', 'active_cores'])

    def get_core_map(self, pids):
        mapping = {}
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", 'r') as f:
                    mapping[pid] = int(f.read().split()[38])
            except: pass
        return mapping

    def run(self):
        print("[INFO] Monitor Started (Safe Mode: No Spin Logic).")
        last_heartbeat = 0
        
        # --- AUTO-EXIT STATE ---
        has_seen_pids = False
        cooldown_counter = 0 
        
        try:
            while True:
                time.sleep(0.2) 
                
                # 1. PIDs & Perf
                pids = self.io_detector.get_pids()
                
                # --- AUTO-EXIT LOGIC START ---
                if not pids:
                    if has_seen_pids:
                        # We saw the simulation running, now it's gone.
                        # Wait a few cycles to be sure (debounce), then exit.
                        cooldown_counter += 1
                        if cooldown_counter >= 5: # ~1 second after finish
                            print(f"\n[INFO] Simulation finished. Exiting.")
                            # Trigger the final energy print manually before breaking
                            print(f"[RESULT] TOTAL_ENERGY_JOULES: {self.total_joules:.2f}")
                            break
                    
                    # If we haven't seen PIDs yet, just wait.
                    continue
                
                # We found PIDs, so the simulation is active. Reset counters.
                has_seen_pids = True
                cooldown_counter = 0
                # --- AUTO-EXIT LOGIC END ---

                self.perf_manager.update(pids)

                # 2. Get Data
                perf_data = self.perf_manager.sample_all()
                
                # 3. Check IO
                dt = time.time() - self.last_time
                is_io = self.io_detector.check_io(pids, dt)
                
                # 4. Power
                self.rapl_file.seek(0)
                curr_energy = int(self.rapl_file.read())
                
                # Handle Wrap-around safely
                diff = curr_energy - self.last_energy
                if diff < 0: diff += 262143328850 # Approximate max for typical RAPL
                
                joules = diff / 1e6
                pwr = joules / dt if dt > 0 else 0
                
                self.total_joules += joules # Accumulate
                self.last_energy = curr_energy
                self.last_time = time.time()

                # 5. Per-Core Logic (Simplified)
                core_map = self.get_core_map(pids)
                rank_metrics = []
                
                for pid, (ipc, mpki) in perf_data.items():
                    if pid not in core_map: continue
                    core = core_map[pid]
                    
                    if is_io:
                        phase = "IO_DISK"
                        freq = FREQ_MIN
                    elif ipc < 1.0 and mpki > MPKI_MEM_THRESHOLD:
                        phase = "MEMORY"
                        freq = FREQ_MID
                    else:
                        phase = "COMPUTE"
                        freq = FREQ_MAX
                    
                    self.controller.set_freq(core, freq)
                    rank_metrics.append(RankMetrics(pid, core, ipc, mpki, phase))

                # 6. Log
                avg_ipc = sum(r.ipc for r in rank_metrics)/len(rank_metrics) if rank_metrics else 0
                global_phase = "IO_DISK" if is_io else "MIXED"
                self.writer.writerow([f"{time.time():.4f}", global_phase, f"{avg_ipc:.2f}", f"{pwr:.1f}", len(rank_metrics)])
                
                # 7. Heartbeat
                if time.time() - last_heartbeat > 5.0:
                    counts = Counter([r.phase for r in rank_metrics])
                    phases_str = ", ".join([f"{k}:{v}" for k,v in counts.items()])
                    print(f"[HEARTBEAT] Pwr: {pwr:.1f}W | IPC: {avg_ipc:.2f}")
                    print(f"            Breakdown: [{phases_str}]")
                    sys.stdout.flush()
                    last_heartbeat = time.time()

        except KeyboardInterrupt:
            # Still catch manual kills from test.sh just in case
            print(f"\n[RESULT] TOTAL_ENERGY_JOULES: {self.total_joules:.2f}")
        finally:
            self.perf_manager.update([])
            self.controller.close()
            self.csv.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, default="0-30")
    args = parser.parse_args()
    
    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e+1))
        else:
            cores.append(int(part))
            
    NativeMonitor(cores).run()
