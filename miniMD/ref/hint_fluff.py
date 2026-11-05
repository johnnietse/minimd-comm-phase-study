#!/usr/bin/env python3
"""
INTELLIGENT Communication Phase Monitor - Version 19.0 (Side-Channel Edition)
Features: 
1. "Side-Channel" Hardware Counters (dTLB, Branch, L1, Stalls)
2. Fingerprint-based Phase Detection
3. Persistent Hint File Handle & Robust Error Handling
"""
import sys
import subprocess
import time
import csv
import os
import math
import statistics
import fcntl
from datetime import datetime
from dataclasses import dataclass
from typing import List
from collections import deque
import numpy as np

# ---------------------- CONFIGURATION ----------------------
HINT_FILE = "/dev/shm/minimd_phase_hint"
SAMPLE_INTERVAL = 0.5
PERF_BIN = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
NETWORK_INTERFACES = ["ib0", "ib1", "eth0", "eth1", "eno1", "ens1"]
MBPS_CONVERSION_FACTOR = 1048576.0

# ---------------------- METRIC CLASSES ----------------------
@dataclass
class SystemMetrics:
    timestamp: float
    pkg_power: float
    dram_power: float
    net_rx_mbps: float
    net_tx_mbps: float
    active_ranks: int
    ctx_switches: float

@dataclass
class PerfMetrics:
    ipc: float
    dtlb_mpki: float       # dTLB Misses Per Kilo Instruction
    branch_miss_rate: float # Percentage of branches missed
    l1_mpki: float         # L1 Cache Misses Per Kilo Instruction
    backend_stall_pct: float # % of cycles stalled waiting for resources

class MetricsCollector:
    def __init__(self, config):
        self.config = config
        self.files = {
            'net': open("/proc/net/dev", 'r'),
            'rapl': open(config['rapl_path'], 'r') if os.path.exists(config['rapl_path']) else None
        }
        
        self.proc_handles = {}
        # Initial Readings
        init_pkg = self._read_rapl(self.files['rapl']) if self.files['rapl'] else 0
        init_rx, init_tx = self._read_net()
        
        self.prev = {
            'time': time.time(),
            'pkg_e': init_pkg, 
            'net_r': init_rx, 'net_s': init_tx, 
            'ctx': 0
        }
        self.perf_process = None
        # Default perf state
        self.perf_data = PerfMetrics(0.0, 0.0, 0.0, 0.0, 0.0)

    def start_perf_streaming(self, pids):
        if self.perf_process or not pids: return
        pid_str = ",".join(map(str, pids))
        
        # --- SIDE CHANNEL EVENTS ---
        # 1. instructions/cycles: for IPC
        # 2. dTLB-load-misses: The "Pointer Chase" Detector (Memory Bound)
        # 3. branch-misses/branches: The "Spaghetti Code" Detector (Comm Logic)
        # 4. L1-dcache-load-misses: The "Data Hungry" Detector (Streaming)
        # 5. stalled-cycles-backend: The "Pipeline" Detector
        events = [
            "instructions:u", "cycles:u",
            "dTLB-load-misses:u",
            "branch-misses:u", "branches:u",
            "L1-dcache-load-misses:u",
            "stalled-cycles-backend:u"
        ]
        
        cmd = [self.config['perf_bin'], "stat", "-I", "50", 
               "-e", ",".join(events),
               "-p", pid_str, "-x", ","]
        
        try:
            self.perf_process = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
            fd = self.perf_process.stderr.fileno() 
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        except Exception as e:
            print(f"[ERROR] Perf stream failed: {e}")

    def read_perf(self) -> PerfMetrics:
        if self.perf_process is None: return self.perf_data
        
        # Accumulators
        cnt = {'inst': 0, 'cyc': 0, 'dtlb': 0, 'b_miss': 0, 'branch': 0, 'l1': 0, 'stall': 0}
        data_found = False
        
        try:
            while True:
                line = self.perf_process.stderr.readline()
                if not line: break
                parts = line.strip().split(',')
                if len(parts) < 3: continue
                try:
                    val = int(parts[1])
                    evt = parts[3]
                    
                    if "instructions" in evt: cnt['inst'] = val
                    elif "cycles" in evt and "stalled" not in evt: cnt['cyc'] = val
                    elif "dTLB-load-misses" in evt: cnt['dtlb'] = val
                    elif "branch-misses" in evt: cnt['b_miss'] = val
                    elif "branches" in evt: cnt['branch'] = val
                    elif "L1-dcache" in evt: cnt['l1'] = val
                    elif "stalled-cycles-backend" in evt: cnt['stall'] = val
                    data_found = True
                except: continue
        except: pass
        
        if not data_found: return self.perf_data

        # --- NORMALIZE METRICS ---
        instr = max(1, cnt['inst'])
        cycles = max(1, cnt['cyc'])
        
        ipc = instr / cycles
        dtlb_mpki = (cnt['dtlb'] / instr) * 1000.0
        l1_mpki = (cnt['l1'] / instr) * 1000.0
        branch_miss_rate = (cnt['b_miss'] / max(1, cnt['branch'])) * 100.0
        stall_pct = (cnt['stall'] / cycles) * 100.0
        
        self.perf_data = PerfMetrics(ipc, dtlb_mpki, branch_miss_rate, l1_mpki, stall_pct)
        return self.perf_data

    def sample(self, pids) -> tuple[SystemMetrics, PerfMetrics]:
        now = time.time()
        dt = now - self.prev['time']
        if dt <= 0: dt = 0.2
        
        if not self.perf_process: self.start_perf_streaming(pids)
        perf_m = self.read_perf()
        
        # Power & Net
        curr_pkg = self._read_rapl(self.files['rapl'])
        pkg_watts = self._safe_delta(curr_pkg, self.prev['pkg_e'], "rapl") / 1e6 / dt
        
        curr_rx, curr_tx = self._read_net()
        net_rx = self._safe_delta(curr_rx, self.prev['net_r']) / dt / MBPS_CONVERSION_FACTOR
        net_tx = self._safe_delta(curr_tx, self.prev['net_s']) / dt / MBPS_CONVERSION_FACTOR
        
        # Context Switches
        curr_ctx = self._get_ctx(pids)
        ctx_rate = self._safe_delta(curr_ctx, self.prev['ctx']) / dt
        
        self.prev.update({'time': now, 'pkg_e': curr_pkg, 'net_r': curr_rx, 'net_s': curr_tx, 'ctx': curr_ctx})
        
        sys_m = SystemMetrics(now, pkg_watts, 0.0, net_rx, net_tx, len(pids), ctx_rate)
        return sys_m, perf_m

    def _safe_delta(self, curr, prev, name=""):
        if curr >= prev: return curr - prev
        max_val = 2**32 if "rapl" in name else 2**64
        diff = (max_val - prev) + curr
        return 0 if diff > 1e10 else diff

    def _read_rapl(self, h): h.seek(0); return int(h.read().strip()) if h else 0
    def _read_net(self):
        self.files['net'].seek(0)
        rx, tx = 0, 0
        for l in self.files['net'].readlines()[2:]:
            p = l.split()
            if p[0].strip(":") in NETWORK_INTERFACES: rx += int(p[1]); tx += int(p[9])
        return rx, tx
    def _get_ctx(self, pids):
        t = 0
        for pid in pids:
            try:
                with open(f"/proc/{pid}/status") as f:
                    for l in f:
                        if "ctxt_switches" in l: t += int(l.split()[1])
            except: pass
        return t

# ---------------------- PHASE ANALYZER ----------------------
class PhaseAnalyzer:
    def __init__(self, use_hints=False):
        self.use_hints = use_hints
        self.hint_handle = None
        self.last_known_hint = None
        
        # -- CALIBRATED THRESHOLDS --
        self.thresh = {
            'DTLB_MPKI_HIGH': 0.05,  # Above this = Memory Random Access
            'BRANCH_MISS_HIGH': 2.0, # Above 2% = Complex Logic (Comm)
            'L1_MPKI_HIGH': 15.0,    # High L1 misses = Streaming
            'STALL_PCT_HIGH': 50.0,  # CPU stuck waiting = Memory
            'IPC_COMPUTE': 1.2       # High IPC = Compute
        }

    def _get_hint(self):
        try:
            if self.hint_handle is None:
                if os.path.exists(HINT_FILE): 
                    self.hint_handle = open(HINT_FILE, "r")
                else: 
                    return None  # No file yet
            
            self.hint_handle.seek(0)
            hint = self.hint_handle.read().strip()
            
            if not hint:
                # If file is empty (race condition), return previous valid hint
                # If we have NO history, default to COMMUNICATION (safer for energy)
                return self.last_known_hint if self.last_known_hint else "COMMUNICATION"
            
            self.last_known_hint = hint
            return hint
        except: 
            return self.last_known_hint

    def detect_phase(self, sys: SystemMetrics, perf: PerfMetrics) -> tuple[str, dict]:
        # 1. ORACLE MODE (HINTS)
        if self.use_hints:
            hint = self._get_hint()
            if hint: return hint, {"reasons": "Oracle Hint"}

        reasons = []
        phase = "COMPUTE" # Default Assumption

        # --- SIDE CHANNEL ATTACK LOGIC ---
        
        # A. MEMORY BOUND CHECK (The Pointer Chase)
        # Signature: High dTLB misses (random access) OR Massive Backend Stalls
        if perf.dtlb_mpki > self.thresh['DTLB_MPKI_HIGH']:
            phase = "MEMORY_BOUND"
            reasons.append(f"High dTLB ({perf.dtlb_mpki:.2f})")
        
        elif perf.backend_stall_pct > self.thresh['STALL_PCT_HIGH'] and perf.ipc < 1.0:
            phase = "MEMORY_BOUND"
            reasons.append(f"High Stalls ({perf.backend_stall_pct:.0f}%)")

        # B. COMMUNICATION CHECK (The Spaghetti Code & Stream)
        # Signature 1: High Branch Misses (MPI logic complexity)
        # Signature 2: High L1 Misses but Low dTLB (Linear buffer copying)
        # Signature 3: Network Traffic (Obvious one)
        elif perf.branch_miss_rate > self.thresh['BRANCH_MISS_HIGH']:
            phase = "COMMUNICATION"
            reasons.append(f"High BranchMiss ({perf.branch_miss_rate:.1f}%)")
            
        elif perf.l1_mpki > self.thresh['L1_MPKI_HIGH'] and perf.dtlb_mpki < self.thresh['DTLB_MPKI_HIGH']:
            phase = "COMMUNICATION"
            reasons.append(f"Streaming Copy (L1:{perf.l1_mpki:.0f})")
            
        elif (sys.net_rx_mbps + sys.net_tx_mbps) > 50.0:
            phase = "COMMUNICATION"
            reasons.append(f"Network Traffic")

        # C. COMPUTE CONFIRMATION
        # If we haven't triggered Mem or Comm, we double check it looks like compute
        else:
            if perf.ipc > self.thresh['IPC_COMPUTE']:
                reasons.append(f"High IPC ({perf.ipc:.2f})")
            else:
                reasons.append("Default/Low Activity")

        return phase, {"reasons": ", ".join(reasons), "scores": f"dTLB:{perf.dtlb_mpki:.2f} BrMiss:{perf.branch_miss_rate:.1f}%"}

# ---------------------- MAIN MONITOR APP ----------------------
class IntelligentMonitorv19:
    def __init__(self, args):
        self.config = {'rapl_path': RAPL_PATH, 'perf_bin': PERF_BIN}
        self.collector = MetricsCollector(self.config)
        self.analyzer = PhaseAnalyzer(use_hints=args.hints)
        self.filename = "test.csv"
        
    def get_miniMD_pids(self):
        try:
            r = subprocess.run(["pgrep", "miniMD_openmpi"], capture_output=True, text=True)
            if r.stdout.strip(): return [int(p) for p in r.stdout.split()]
        except: pass
        return []

    def run(self):
        print(f"[INFO] V19 Side-Channel Monitor. Hints: {self.analyzer.use_hints}")
        if self.analyzer.use_hints: os.environ["MINIMD_HINT_MODE"] = "1"

        while True:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Waiting for miniMD...")
            pids = []
            while not pids:
                pids = self.get_miniMD_pids()
                time.sleep(1.0)
            
            print(f"[INFO] Attached to {len(pids)} ranks. Logging to {self.filename}")
            
            with open(self.filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'timestamp', 'phase', 'ipc', 'dtlb_mpki', 'branch_miss_pct', 
                    'l1_mpki', 'stall_pct', 'pkg_power', 'net_total', 'reasons'
                ])
                writer.writeheader()
                
                try:
                    while True:
                        t0 = time.time()
                        
                        # Check if alive
                        pids = self.get_miniMD_pids()
                        if not pids: break
                        
                        # Sample & Detect
                        sys_m, perf_m = self.collector.sample(pids)
                        phase, details = self.analyzer.detect_phase(sys_m, perf_m)
                        
                        writer.writerow({
                            'timestamp': f"{sys_m.timestamp:.4f}",
                            'phase': phase,
                            'ipc': f"{perf_m.ipc:.2f}",
                            'dtlb_mpki': f"{perf_m.dtlb_mpki:.2f}",
                            'branch_miss_pct': f"{perf_m.branch_miss_rate:.2f}",
                            'l1_mpki': f"{perf_m.l1_mpki:.1f}",
                            'stall_pct': f"{perf_m.backend_stall_pct:.1f}",
                            'pkg_power': f"{sys_m.pkg_power:.1f}",
                            'net_total': f"{sys_m.net_rx_mbps + sys_m.net_tx_mbps:.1f}",
                            'reasons': details['reasons']
                        })
                        
                        # Console Feedback
                        print(f"\r[{phase[:4]}] IPC:{perf_m.ipc:.2f} dTLB:{perf_m.dtlb_mpki:.2f} BrMiss:{perf_m.branch_miss_rate:.1f}% " + 
                              f"L1:{perf_m.l1_mpki:.0f} {details['reasons'][:40]}", end="")
                        
                        time.sleep(max(0, SAMPLE_INTERVAL - (time.time() - t0)))

                    return
                        
                except KeyboardInterrupt: return
            print("\n[INFO] miniMD finished.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--hints', action='store_true')
    args = parser.parse_args()
    IntelligentMonitorv19(args).run()
