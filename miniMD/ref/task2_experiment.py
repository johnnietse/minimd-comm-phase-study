#!/usr/bin/env python3
import os
import time
import subprocess
import fcntl
import argparse
import sys
from typing import List

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PERF_BIN = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
if not os.path.exists(PERF_BIN):
    PERF_BIN = "perf"

APP_PGREP_PATTERN = "miniMD_openmpi"
WCHAR_THRESHOLD_BPS = 50 * 1024 * 1024 
IGNORE_IO_FIRST_N_SEC = 5.0

# --- SAFETY TUNING ---
LOOP_SLEEP_SEC = 0.5 
IPC_COMPUTE_THRESHOLD = 0.3
MPKI_MEM_THRESHOLD = 50.0      

FREQ_MAX = 2000000 
FREQ_MID = 1600000
FREQ_MIN = 1200000

# ==============================================================================
# 1. PERF STREAMING
# ==============================================================================

class SingleProcessPerf:
    def __init__(self, pid):
        self.pid = pid
        self.process = None
        self.last_ipc = 0.0
        self.last_mpki = 0.0
        self._start()

    def _start(self):
        # We use -I 500 to match our 0.5s loop sleep
        cmd = [PERF_BIN, "stat", "-I", "500", 
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
        
        # Read from pipe without blocking
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
# 2. OPTIMIZED PID FINDER (CACHED + SIGNAL CHECK)
# ==============================================================================

class PidManager:
    def __init__(self):
        self.pids = []
        
    def get_pids(self) -> List[int]:
        # 1. Fast Check: Are cached PIDs still alive?
        if self.pids:
            alive = []
            for pid in self.pids:
                try:
                    # Signal 0 checks if process exists and we have permission
                    os.kill(pid, 0) 
                    alive.append(pid)
                except OSError: pass
            
            # If we lost some Pids, update list
            if len(alive) < len(self.pids):
                self.pids = alive
            
            # If we still have PIDs, return them. Don't run pgrep.
            if self.pids:
                return self.pids

        # 2. Slow Check: Only run pgrep if we have NO pids
        try:
            out = subprocess.check_output(
                ["pgrep", "-u", os.environ.get("USER"), "-f", APP_PGREP_PATTERN],
                text=True
            ).strip()
            if out:
                self.pids = sorted({int(x) for x in out.split() if x.isdigit()})
        except: 
            self.pids = []
            
        return self.pids

# ==============================================================================
# 3. PERSISTENT READERS (IO & CORES)
# ==============================================================================

class IODetector:
    def __init__(self):
        self.handles = {}
        self.last_wchar = {}
        self.t0 = time.time()

    def check_io(self, pids, dt):
        # Update handles lazily
        for pid in pids:
            if pid not in self.handles:
                try: self.handles[pid] = open(f"/proc/{pid}/io", "r")
                except: pass
        
        total_dwchar = 0
        # Iterate over open handles
        for pid, f in list(self.handles.items()):
            if pid not in pids: 
                f.close(); del self.handles[pid]; continue
            try:
                f.seek(0)
                for line in f:
                    if line.startswith("wchar:"):
                        val = int(line.split()[1])
                        prev = self.last_wchar.get(pid, val)
                        if val >= prev: total_dwchar += (val - prev)
                        self.last_wchar[pid] = val
                        break
            except: pass

        bw = total_dwchar / max(dt, 1e-6)
        t = time.time() - self.t0
        return (bw >= WCHAR_THRESHOLD_BPS) and (t >= IGNORE_IO_FIRST_N_SEC)

class CoreMapper:
    def __init__(self):
        self.cache = {} 

    def get_map(self, pids):
        # Update cache for new PIDs only
        for pid in pids:
            if pid not in self.cache:
                try:
                    with open(f"/proc/{pid}/stat", 'r') as f:
                        # Field 38 is core_id (0-indexed in array is 38, typically 39th col)
                        self.cache[pid] = int(f.read().split()[38])
                except: pass
        return self.cache

# ==============================================================================
# 4. CONTROLLER (PERSISTENT HANDLES)
# ==============================================================================

class DirectFrequencyController:
    def __init__(self, allowed_cores: List[int], monitor_only=False):
        self.handles = {}
        self.last_freq = {} 
        self.monitor_only = monitor_only
        
        if not self.monitor_only:
            for c in allowed_cores:
                try:
                    f = open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed", 'w')
                    self.handles[c] = f
                    self.last_freq[c] = 0
                except: pass

    def set_freq(self, core, freq):
        if self.monitor_only: return
        
        # Zero-overhead check: Is freq already set?
        if core in self.handles and self.last_freq.get(core) != freq:
            try:
                f = self.handles[core]
                f.seek(0)
                f.write(str(freq))
                f.flush()
                self.last_freq[core] = freq
            except: pass
    
    def close(self):
        for h in self.handles.values(): 
            try: h.close()
            except: pass

# ==============================================================================
# 5. MAIN MONITOR
# ==============================================================================

class NativeMonitor:
    def __init__(self, cores, heartbeat=False, monitor_only=False):
        self.controller = DirectFrequencyController(cores, monitor_only=monitor_only)
        self.io_detector = IODetector()
        self.core_mapper = CoreMapper()
        self.perf_manager = PerfManager()
        self.pid_manager = PidManager()
        
        self.heartbeat = heartbeat
        self.monitor_only = monitor_only
        
    def run(self):
        mode = "PASSIVE" if self.monitor_only else "ACTIVE"
        if self.heartbeat: print(f"[INFO] Monitor Started. Mode: {mode}")
        
        has_seen_activity = False
        empty_cycles = 0
        last_time = time.time()

        try:
            while True:
                time.sleep(LOOP_SLEEP_SEC) 
                
                # 1. PIDs (Optimized: Check cache, fallback to pgrep)
                pids = self.pid_manager.get_pids()
                
                # Auto-Exit Logic
                if pids:
                    has_seen_activity = True
                    empty_cycles = 0
                elif has_seen_activity:
                    empty_cycles += 1
                    if empty_cycles >= 4:
                        if self.heartbeat: print("[INFO] Simulation ended.")
                        break
                    continue
                if not pids: continue

                # 2. Update Managers
                self.perf_manager.update(pids)
                
                # 3. Read Metrics (Pure Reads)
                perf_data = self.perf_manager.sample_all() 
                
                now = time.time()
                dt = now - last_time
                last_time = now
                
                is_io = self.io_detector.check_io(pids, dt)
                core_map = self.core_mapper.get_map(pids) # Cached
                
                # 4. Control Logic (Pure memory comparisons)
                total_ipc = 0
                count = 0
                phase_counts = {"COMPUTE":0, "MEMORY":0, "IO_DISK":0}
                
                for pid, (ipc, mpki) in perf_data.items():
                    if pid not in core_map: continue
                    core = core_map[pid]
                    
                    freq = FREQ_MAX
                    phase = "COMPUTE"

                    if is_io:
                        phase = "IO_DISK"
                        freq = FREQ_MAX
                    elif ipc < IPC_COMPUTE_THRESHOLD and mpki > MPKI_MEM_THRESHOLD:
                        phase = "MEMORY"
                        freq = FREQ_MAX
                    
                    self.controller.set_freq(core, freq)
                    
                    total_ipc += ipc
                    count += 1
                    phase_counts[phase] += 1

                if self.heartbeat and count > 0:
                    avg_ipc = total_ipc / count
                    print(f"IPC: {avg_ipc:.2f} | IO: {is_io} | {phase_counts}")

        except KeyboardInterrupt:
            pass
        finally:
            self.perf_manager.update([]) 
            self.controller.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, default="0-30")
    parser.add_argument('--heartbeat', action='store_true')
    parser.add_argument('--monitor-only', action='store_true')
    args = parser.parse_args()
    
    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e+1))
        else:
            cores.append(int(part))
            
    NativeMonitor(cores, heartbeat=args.heartbeat, monitor_only=args.monitor_only).run()
