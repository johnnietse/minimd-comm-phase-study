#!/usr/bin/env python3
"""
INTELLIGENT Communication Phase Monitor for miniMD - Version 17.0
OPTIMIZED for LOW OVERHEAD (<2%) using PERF STREAMING
"""
import sys
import subprocess
import time
import csv
import os
import math
import statistics
import glob
import fcntl
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from dataclasses import dataclass
from typing import List
from collections import deque

import numpy as np
import fcntl


# ---------------------- CONFIGURATION ----------------------

CMD = ["mpirun", "--oversubscribe", "-np", "32", "./miniMD_openmpi", "i", "in.lj.miniMD"]
LOG_FILE = f"comm_phase_monitor_log.csv"
SUMMARY_FILE = f"comm_phase_summary_log.txt"

# Sample interval
SAMPLE_INTERVAL = 0.05

PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_DRAM_PATH = "/sys/class/powercap/intel-rapl:0:0/energy_uj"

FREQ_MAX = "2000000"
FREQ_MIN = "1600000"

MBPS_CONVERSION_FACTOR = 1048576.0

# Thresholds
IPC_THRESHOLD = 1.6
MISS_THRESHOLD = 0.30
POWER_MARGIN_THRESHOLD = 1.5
TICKS_PER_SECOND = 100
THREAD_APP_LIMIT = 0.25
MAX_CTX_RATE = 1e6



# Network interface to monitor
NETWORK_INTERFACES = ["ib0", "ib1", "eth0", "eth1", "eno1", "ens1"]

EMPIRICAL_SCALING_DATA = {
    2: {"comm_pct": 23.7, "compute_pct": 62.5, "force_pct": 62.5, "neigh_pct": 12.6},
    4: {"comm_pct": 40.2, "compute_pct": 47.5, "force_pct": 47.5, "neigh_pct": 11.7},
    8: {"comm_pct": 75.5, "compute_pct": 14.9, "force_pct": 14.9, "neigh_pct": 9.3},
    16: {"comm_pct": 87.5, "compute_pct": 4.6, "force_pct": 4.6, "neigh_pct": 7.7},
    32: {"comm_pct": 96.9, "compute_pct": 1.9, "force_pct": 1.9, "neigh_pct": 0.3},
    64: {"comm_pct": 97.8, "compute_pct": 0.8, "force_pct": 0.8, "neigh_pct": 0.2}
}

# System metric data object 
@dataclass
class SystemMetrics:
    timestamp: float

    # CPU / Perf
    ipc: float
    miss_rate: float
    cache_ref: float
    cache_miss: float
    cpu_util_all: List[float]   # [user, system, idle, iowait]
    cpu_total_util: float
    iowait_pct: float

    # Power
    pkg_power: float
    dram_power: float

    # Network
    net_rx_mbps: float
    net_tx_mbps: float

    # IO

    # Process Context
    active_ranks: int
    ctx_switches: float
    sync_variance: float

    # Derived/Normalized
    power_per_rank: float = 0.0
    cpu_per_rank: float = 0.0
    effective_cpu_util: float = 0.0


class MetricsCollector:
    def __init__(self, config):
        self.config = config

        # Persitent file handles for minimal sys call overhead
        self.files = {
            'stat': open("/proc/stat", 'r'),
            'net': open("/proc/net/dev", 'r'),
            'rapl': open(config['rapl_path'], 'r') if os.path.exists(config['rapl_path']) else None
        }

        if config.get('dram_path') and os.path.exists(config['dram_path']):
            self.files['dram'] = open(config['dram_path'], 'r')

        self.proc_handles = {}

        # Take an initial reading so the first sample isn't a massive spike
        init_pkg = self._read_rapl(self.files['rapl']) if self.files['rapl'] else 0
        init_dram = self._read_rapl(self.files['dram']) if 'dram' in self.files else 0

        init_rx, init_tx = self._read_net()
        
        # state tracking for deltas in calculations
        self.prev = {
            'time': time.time(),
            'pkg_e': init_pkg,
            'dram_e': init_dram,
            'net_r': init_rx,  
            'net_s': init_tx, 
            'ctx': 0, 
            'cpu': [0]*8, #[user time, nice (low prioroity), system time, idle time , iowait, hard interupt time, soft interupt time ]
            'per_proc_cpu': {}
        }

        # perf streaming
        self.perf_process = None
        self.perf_data = {'ipc': 0.0, 'miss': 0.0}

    def start_perf_streaming(self, pids):
        """ Start the continous perf process to reduce overhead"""
        if self.perf_process or not pids: return

        pid_str = ",".join(map(str, pids))
        cmd = [self.config['perf_bin'], "stat", "-I", "1000", 
       "-e", "cycles:u,instructions:u,cache-misses:u,stalled-cycles-backend:u", # Added stalled-cycles
       "-p", pid_str, "-x", ","]
        
        try:
            self.perf_process = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
            
            # FIX: Access .stderr before .fileno()
            file_descriptor = self.perf_process.stderr.fileno() 
            
            fl = fcntl.fcntl(file_descriptor, fcntl.F_GETFL)
            fcntl.fcntl(file_descriptor, fcntl.F_SETFL, fl | os.O_NONBLOCK) # Non-blocking read
        except Exception as e:
            print(f"[ERROR] Perf stream failed: {e}")
    
    def read_perf(self):
        """Reads the latest available data from the persistent perf stream"""

        if self.perf_process == None:
            return 0.0, 0.0, 0.0, 0.0
        
        cycles = instr = refs = misses = 0
        data_found = False

        try:
            # Read all lines in the buffer
            while True:
                line = self.perf_process.stderr.readline()
                if not line: break
                parts = line.strip().split(',')
                if len(parts) < 3: continue

                # Timestamp, value, unit, event

                try:
                    val = int(parts[1])
                    event = parts[3]

                    if "cycles" in event: cycles = val
                    elif "instructions" in event: instr = val
                    elif "misses" in event: misses = val
                    elif "references" in event: refs = val
                    data_found = True
                except:
                    continue
        except:
            pass

        # If unable to find new data in this iteration, return cached ipc and miss rate
        if not data_found:
            return self.perf_data['ipc'], self.perf_data['miss'], 0.0, 0.0

        ipc = instr / cycles if cycles > 0 else 0.0
        miss_rate = misses / refs if refs > 0 else 0.0

        # cache the resuylts
        self.perf_data['ipc'] = ipc
        self.perf_data['miss'] = miss_rate

        return ipc, miss_rate, refs, misses

    def sample(self, pids) -> SystemMetrics:
        now = time.time()
        dt = now - self.prev['time']
        if dt <= 0: 
            dt = 0.2

        # Make sure the perf stream is connected
        if not self.perf_process:
            self.start_perf_streaming(pids)

        # 1. Collect perf stream data
        ipc, miss_rate, cache_ref, cache_miss = self.read_perf()

        # Read the raw counter values (use seek(0) for overhead reduction)
        # 2. Get system wide CPU metrics
        self.files['stat'].seek(0)
        cpu_line = self.files['stat'].readline().split()
        curr_cpu = [int(x) for x in cpu_line[1:9]]

        # compute delats
        deltas = [self._safe_delta(c, p, f"cpu_{i}") for i, (c, p) in enumerate(zip(curr_cpu, self.prev['cpu']))]
        total_cpu_delta = sum(deltas)

        if total_cpu_delta > 0:
            cpu_util_all = [100 * d / total_cpu_delta for d in deltas]
            # get the total am,ount besides for idling and io to see how much time was spent doing active work on the cpu
            cpu_total_util = 100 - cpu_util_all[3] - cpu_util_all[4]
        else:
            cpu_util_all = [0] * 8
            cpu_total_util = 0.0

        # 3. THE POWER
        # package poiwer
        curr_pkg = self._read_rapl(self.files['rapl'])
        delta_pkg = self._safe_delta(curr_pkg, self.prev['pkg_e'], "rapl_pkg")
        pkg_watts = delta_pkg / 1e6 / dt

        # dram power (assign state now since the it is the only conditional state)
        dram_watts = 0.0
        if 'dram' in self.files:
            curr_dram = self._read_rapl(self.files['dram'])
            delta_dram = self._safe_delta(curr_dram, self.prev['dram_e'], "rapl_dram")
            dram_watts = delta_dram / 1e6 / dt
            self.prev['dram_e'] = curr_dram
            
        # 4. NETWORK data
        curr_rx, curr_tx = self._read_net()
        delta_rx = self._safe_delta(curr_rx, self.prev['net_r'], "net_rx")
        delta_tx = self._safe_delta(curr_tx, self.prev['net_s'], "net_tx")

        # Convwert in MBs
        net_rx_mbps = delta_rx / dt / MBPS_CONVERSION_FACTOR
        net_tx_mbps = delta_tx / dt / MBPS_CONVERSION_FACTOR

        # 5. Process level metrics (context and sync variance)
        curr_ctx, curr_proc_times = self._get_proc_metrics(pids)

        delta_ctx = self._safe_delta(curr_ctx, self.prev['ctx'], "ctx")
        ctx_rate = delta_ctx / dt

        # sync variance
        sync_var = self._calc_sync_var(curr_proc_times, self.prev['per_proc_cpu'])

        # 6. UPDATE STATE
        self.prev['time'] = now
        self.prev['cpu'] = curr_cpu
        self.prev['pkg_e'] = curr_pkg
        self.prev['net_r'] = curr_rx
        self.prev['net_s'] = curr_tx
        self.prev['ctx'] = curr_ctx
        self.prev['per_proc_cpu'] = curr_proc_times

        iowait = cpu_util_all[4] if len(cpu_util_all) > 4 else 0.0

        return SystemMetrics(
            timestamp=now,
            ipc=ipc,
            miss_rate=miss_rate,
            cache_ref=cache_ref,
            cache_miss=cache_miss,
            cpu_util_all=cpu_util_all,
            cpu_total_util=cpu_total_util,
            iowait_pct=iowait,
            pkg_power=pkg_watts,
            dram_power=dram_watts,
            net_rx_mbps=net_rx_mbps,
            net_tx_mbps=net_tx_mbps,
            active_ranks=len(pids),
            ctx_switches=ctx_rate,
            sync_variance=sync_var
        )



    # ---------------------- Helper Functions Begin----------------------
    # def _safe_delta(self, current, previous, counter_name=""):
    #     if current >= previous: return current - previous
    #     max_value = 2**32 if "energy" in counter_name or "rapl" in counter_name.lower() else 2**64
    #     return (max_value - previous) + current
    def _safe_delta(self, current, previous, counter_name=""):
        if current >= previous:
            return current - previous
        
        # RAPL counters (energy_uj) are usually 32-bit unsigned
        # standard counters (like /proc/net/dev) are usually 64-bit
        is_rapl = "energy" in counter_name or "rapl" in counter_name.lower()
        max_val = 2**32 if is_rapl else 2**64
        
        diff = (max_val - previous) + current
        
        # Sanity check: If the jump is impossibly large (e.g. > 10000 Watts equivalent),
        # it might be a driver reset, not a wrap-around. Ignore this sample.
        # 10000 Joules in 0.2s = 50,000 Watts.
        if is_rapl and diff > 10000000000: 
            return 0 
            
        return diff
    def _read_rapl(self, handle):
        """Read rapl without reopeing the file"""
        if not handle: 
            return 0
        
        try:
            handle.seek(0)
            return int(handle.read().strip())
        except: return 0 

    def _read_net(self):
        """ Getv the sum of the bytes for all bnetwork interfaces"""

        total_rx, total_tx = 0, 0

        try:
            self.files['net'].seek(0)
            # no need for the header lines
            lines = self.files['net'].readlines()[2:]

            for line in lines:
                parts = line.split()
                if not parts:
                    continue
                iface = parts[0].strip(":")

                if iface in NETWORK_INTERFACES:
                    total_rx += int(parts[1])
                    total_tx += int(parts[9])

        except: 
            pass
        return total_rx, total_tx
    
    def _cleanup_pids(self, pids):
        """ Returns context sqiucthes and cpu time per process"""
        

        # clean up any dead pids
        curr_pids = set(pids)
        for pid in list(self.proc_handles.keys()):
            if pid not in curr_pids:
                try:
                    self.proc_handles[pid]['stat'].close()
                except:
                    pass

                try:
                    self.proc_handles[pid]['status'].close()
                except:
                    pass

                del self.proc_handles[pid]

    def _get_proc_metrics(self, pids):
        # Clean up old PIDs first
        self._cleanup_pids(pids)

        total_ctx = 0
        proc_times = {}

        for pid in pids:
            # 1. Open handles if new PID
            if pid not in self.proc_handles:
                try:
                    self.proc_handles[pid] = {
                        'stat': open(f"/proc/{pid}/stat", 'r'),
                        'status': open(f"/proc/{pid}/status", 'r'),
                    }
                except:
                    continue

            # 2. Get CPU time from stat
            try:
                f_stat = self.proc_handles[pid]['stat']
                f_stat.seek(0)
                data = f_stat.read().split()
                if len(data) > 14: # Basic safety check
                    proc_times[pid] = int(data[13]) + int(data[14])
            except:
                pass
            
            # 3. Get Context Switches from status
            try:
                f_status = self.proc_handles[pid]['status']
                f_status.seek(0)
                for line in f_status:
                    if "voluntary_ctxt_switches" in line:
                        total_ctx += int(line.split()[1])
                    elif "nonvoluntary_ctxt_switches" in line:
                        total_ctx += int(line.split()[1])
            except:
                pass
        
        return total_ctx, proc_times
    
    def _calc_sync_var(self, curr_times, prev_times):
        """Variance of cpu time consumed by each rank in interval"""
        deltas = []

        for pid, val in curr_times.items():
            if pid in prev_times:
                delta = max(val - prev_times[pid], 0)
                deltas.append(delta)
            
        if len(deltas) < 2:
            return 0.0
        
        avg = statistics.mean(deltas)
        if avg == 0: 
            return 0.0
        
        # Get the norm variance
        try:
            variance = statistics.variance(deltas, avg)
            return (variance / avg * 100)
        except:
            return 0.0

   # ---------------------- Helper Functions END----------------------

class PhaseAnalyzer:
    def __init__(self, expected_ranks):
        self.expected_ranks = expected_ranks

        # adaptive history
        self.history = {
            'power': deque(maxlen=100),
            'ipc': deque(maxlen=100),
            'idle': deque(maxlen=100),
            'ctx': deque(maxlen=100),
            'sync': deque(maxlen=100),
            'miss': deque(maxlen=100),
            'phase_seq': deque(maxlen=10)
        }

        # --- OPTIMIZED PARAMETERS (Generated by Scientific Tuner) ---
        self.weights = {
            'W_COMP_PWR': 4.6077,
            'W_COMP_IPC': 4.8152,
            'W_COMP_UTIL': 3.8543,
            'W_COMM_NET': 0.6481,   # Note: Low weight means Net is noisy
            'W_COMM_LOW_U': 3.3410,
            'W_COMM_VAR': 1.1954,
            'W_MEM_MISS': 2.9495,
            'W_MEM_DRAM': 3.8619
        }

        self.thresholds = {
            'T_NET_MBPS': 30.5306,
            'T_EFF_UTIL': 68.2809,
            'T_SYNC_VAR': 33.5608
        }
        # -----------------------------------------------------------

        # intilizae dynamic thresholds
        self.dynamic_thresholds = {
            'power_compute_threshold': 180.0,
            'power_comm_threshold': 140.0,
            'ipc_compute_target': 1.6,
            'miss_rate_high_target': 0.30,
            'idle_comm_threshold': 80.0,
            'ctx_comm_threshold': 1000,
            'sync_variance_threshold': 10.0,
            'power_per_rank': 15.0 
        }

    def _normalize_metrics(self, m:SystemMetrics):
        """Perform rank aware normalization"""
        ranks = m.active_ranks if m.active_ranks > 0 else None
        if not ranks:
            return
        
        total_cpu_cores = 32
        
        # Normalize the power
        m.power_per_rank = m.pkg_power / ranks

        # NOrmalize CPU
        max_possible_util = (ranks / total_cpu_cores) * 100
        m.effective_cpu_util = (m.cpu_total_util / max_possible_util * 100) if max_possible_util > 0 else 0

    def _update_adaptive_thresholds(self, m: SystemMetrics):
        self.history['power'].append(m.pkg_power)
        self.history['ipc'].append(m.ipc)
        self.history['idle'].append(m.cpu_util_all[3] if len(m.cpu_util_all) > 3 else 0)
        self.history['ctx'].append(m.ctx_switches)
        self.history['sync'].append(m.sync_variance)
        self.history['miss'].append(m.miss_rate)
        
        if len(self.history['power']) >= 20 and len(self.history['power']) % 20 == 0:
            try:
                # Power Thresholds
                p25_pwr = np.percentile(self.history['power'], 25)
                p75_pwr = np.percentile(self.history['power'], 75)
                self.dynamic_thresholds['power_compute_threshold'] = p75_pwr * 0.90
                self.dynamic_thresholds['power_comm_threshold'] = p25_pwr * 1.10

                # Performance Thresholds
                self.dynamic_thresholds['ipc_compute_target'] = np.percentile(self.history['ipc'], 75)
                self.dynamic_thresholds['miss_rate_high_target'] = np.percentile(self.history['miss'], 75)
            except: pass

    def detect_statistical_outliers(self, m: SystemMetrics):
        # We keep the outlier logic but rely mostly on the weights now
        if len(self.history['power']) < 10: return 0.0
        try:
            p_mean = np.mean(self.history['power'])
            p_std = np.std(self.history['power'])
            i_mean = np.mean(self.history['idle'])
            i_std = np.std(self.history['idle'])
            
            p_z = (m.pkg_power - p_mean) / p_std if p_std > 0 else 0
            i_z = (m.cpu_util_all[3] - i_mean) / i_std if i_std > 0 else 0
            
            if p_z > 1.5 and i_z < -1.0: return 1.0
            if p_z < -1.5 and i_z > 1.0: return -1.0
            return 0.0
        except: return 0.0

    def _stabilize_phase(self, phase):
        self.history['phase_seq'].append(phase)
        if len(self.history['phase_seq']) >= 3:
            counts = Counter(list(self.history['phase_seq'])[-3:])
            most_common = counts.most_common(1)[0]
            if most_common[1] >= 2 and most_common[0] != phase:
                return most_common[0]
        return phase

    # Zaner mine

    # def detect_phase(self, m: SystemMetrics) -> tuple[str, dict]:
    #     self._normalize_metrics(m)
    #     self._update_adaptive_thresholds(m)

    #     comp_score = 0.0
    #     comm_score = 0.0
    #     mem_score = 0.0
    #     reasons = []
        
    #     total_net = m.net_rx_mbps + m.net_tx_mbps

    #     # --- A. COMPUTE INDICATORS (Optimized Weights) ---
    #     if m.pkg_power > self.dynamic_thresholds['power_compute_threshold']:
    #         comp_score += self.weights['W_COMP_PWR'] # 4.61
    #         reasons.append(f"high power ({m.pkg_power:.0f}W)")
        
    #     if m.ipc > self.dynamic_thresholds['ipc_compute_target']:
    #         comp_score += self.weights['W_COMP_IPC'] # 4.82
    #         reasons.append(f"high ipc ({m.ipc:.2f})")
            
    #     # Tuned Threshold: T_EFF_UTIL (68.28%)
    #     if m.effective_cpu_util > self.thresholds['T_EFF_UTIL']:
    #         comp_score += self.weights['W_COMP_UTIL'] # 3.85
    #         reasons.append(f"high eff_util ({m.effective_cpu_util:.0f}%)")

    #     # --- B. NETWORK / COMM INDICATORS ---
    #     # Tuned Threshold: T_NET_MBPS (30.5 MB/s)
    #     if total_net > self.thresholds['T_NET_MBPS']:
    #         comm_score += self.weights['W_COMM_NET'] # 0.65 (Note: Very Low!)
    #         reasons.append(f"high net ({total_net:.0f}MB/s)")

    #     # "Phantom Compute"
    #     if m.effective_cpu_util < 40.0 and m.pkg_power > 50:
    #         comm_score += self.weights['W_COMM_LOW_U'] # 3.34

    #     # Tuned Threshold: T_SYNC_VAR (33.56%)
    #     if m.sync_variance > self.thresholds['T_SYNC_VAR']:
    #         comm_score += self.weights['W_COMM_VAR'] # 1.20
    #         reasons.append(f"high variance ({m.sync_variance:.1f}%)")

    #     # --- C. OUTLIER ANALYSIS ---
    #     outlier = self.detect_statistical_outliers(m)
    #     if outlier > 0.7: 
    #         comp_score += 2.0
    #         reasons.append("comp_outlier")
    #     elif outlier < -0.7: 
    #         comm_score += 2.0
    #         reasons.append("comm_outlier")

    #     # --- D. MEMORY INDICATORS ---
    #     miss_target = self.dynamic_thresholds['miss_rate_high_target']
    #     if m.ipc < 1.0 and m.miss_rate > miss_target:
    #         mem_score += self.weights['W_MEM_MISS'] # 2.95
    #         reasons.append(f"mem_bound(miss={m.miss_rate:.2f})")
        
    #     if m.dram_power > 10.0:
    #         mem_score += self.weights['W_MEM_DRAM'] # 3.86

    #     # --- E. DECISION ---
    #     if m.pkg_power < 50.0 and m.effective_cpu_util < 10:
    #         phase = "IDLE"
    #     elif mem_score > 2.0:
    #         phase = "MEMORY_BOUND"
    #     elif comm_score > comp_score:
    #         phase = "COMMUNICATION"
    #     else:
    #         phase = "COMPUTE"

    #     # NOTE: Since we tuned MEMORY and COMPUTE to be the same, 
    #     # both will trigger High Performance frequency.
    #     final_phase = self._stabilize_phase(phase)

    #     details = {
    #         'phase': final_phase,
    #         'scores': f"Comp:{comp_score:.1f} Comm:{comm_score:.1f} Mem:{mem_score:.1f}",
    #         'reasons': ', '.join(reasons)
    #     }
    #     return final_phase, details  
    
    # Zaner gem

    def detect_phase(self, m: SystemMetrics) -> tuple[str, dict]:
        self._normalize_metrics(m)
        self._update_adaptive_thresholds(m)
        reasons = []

        # 1. DETECT STORAGE (I/O) [cite: 276]
        # Blueprint: CPU util drops below 30% during I/O bound
        if m.cpu_util_all[4] > 5.0 or (m.cpu_total_util < 30.0 and m.pkg_power < 100):
            return "STORAGE", {"reasons": f"High Iowait {m.cpu_util_all[4]:.1f}%"}

        # 2. DETECT COMMUNICATION [cite: 343]
        # High Network OR Low effective utilization with variance
        if m.net_rx_mbps + m.net_tx_mbps > self.thresholds['T_NET_MBPS']:
            return "COMMUNICATION", {"reasons": "High Net Traffic"}
        
        if m.sync_variance > self.thresholds['T_SYNC_VAR']:
             return "COMMUNICATION", {"reasons": "High MPI Sync Var"}

        # 3. DIFFERENTIATE COMPUTE vs MEMORY [cite: 361]
        # Memory bound: High Cache Miss OR High DRAM Power + Lower IPC
        if m.dram_power > 15.0 or m.miss_rate > 0.40:
             return "MEMORY_BOUND", {"reasons": f"High DRAM Pwr {m.dram_power:.1f}W"}

        # 4. DEFAULT TO COMPUTE [cite: 299]
        return "COMPUTE", {"reasons": "High Eff Util & IPC"}
    
     # Zaner merged
    def detect_phase(self, m: SystemMetrics) -> tuple[str, dict]:
        self._normalize_metrics(m)
        self._update_adaptive_thresholds(m)
        
        reasons = []
        comp_score = 0.0
        comm_score = 0.0
        mem_score = 0.0
        
        total_net = m.net_rx_mbps + m.net_tx_mbps

        # ---------------------------------------------------------
        # 1. PRIORITY CHECK: STORAGE (I/O)
        # ---------------------------------------------------------
        # As per Blueprint: Storage phases are characterized by low CPU utilization (<30%) 
        # and high I/O wait times. We check this first to "gate" the logic.
        if m.cpu_util_all[4] > 5.0 or (m.cpu_total_util < 30.0 and m.pkg_power < 100):
            # Using specific "STORAGE" tag triggers the lowest power state
            return "STORAGE", {"reasons": f"High Iowait {m.cpu_util_all[4]:.1f}% / Low Util"}

        # ---------------------------------------------------------
        # 2. SCORING ANALYSIS (Compute vs. Comm vs. Memory)
        # ---------------------------------------------------------
        
        # --- A. COMPUTE INDICATORS ---
        if m.pkg_power > self.dynamic_thresholds['power_compute_threshold']:
            comp_score += self.weights['W_COMP_PWR']
            reasons.append(f"high power ({m.pkg_power:.0f}W)")
        
        if m.ipc > self.dynamic_thresholds['ipc_compute_target']:
            comp_score += self.weights['W_COMP_IPC']
            reasons.append(f"high ipc ({m.ipc:.2f})")
            
        if m.effective_cpu_util > self.thresholds['T_EFF_UTIL']:
            comp_score += self.weights['W_COMP_UTIL']
            reasons.append(f"high eff_util ({m.effective_cpu_util:.0f}%)")

        # --- B. NETWORK / COMM INDICATORS ---
        # High Network Traffic [cite: 343]
        if total_net > self.thresholds['T_NET_MBPS']:
            comm_score += self.weights['W_COMM_NET']
            reasons.append(f"high net ({total_net:.0f}MB/s)")

        # "Phantom Compute" (High power but doing no real work)
        if m.effective_cpu_util < 40.0 and m.pkg_power > 50:
            comm_score += self.weights['W_COMM_LOW_U']

        # MPI Synchronization Variance [cite: 343]
        if m.sync_variance > self.thresholds['T_SYNC_VAR']:
            comm_score += self.weights['W_COMM_VAR']
            reasons.append(f"high variance ({m.sync_variance:.1f}%)")

        # --- C. MEMORY INDICATORS ---
        # Memory Bound detection based on Miss Rate [cite: 361]
        miss_target = self.dynamic_thresholds['miss_rate_high_target']
        if m.ipc < 1.0 and m.miss_rate > miss_target:
            mem_score += self.weights['W_MEM_MISS']
            reasons.append(f"high miss ({m.miss_rate:.2f})")
        
        # High DRAM Power usage [cite: 361]
        if m.dram_power > 15.0: # Updated threshold from 10.0 to 15.0 based on new findings
            mem_score += self.weights['W_MEM_DRAM']
            reasons.append(f"high dram pwr ({m.dram_power:.1f}W)")

        # --- D. OUTLIER ANALYSIS ---
        outlier = self.detect_statistical_outliers(m)
        if outlier > 0.7: 
            comp_score += 2.0
        elif outlier < -0.7: 
            comm_score += 2.0

        # ---------------------------------------------------------
        # 3. FINAL DECISION
        # ---------------------------------------------------------
        # We compare scores to find the dominant phase behavior.
        
        if mem_score > 2.0:
            # Memory takes precedence over generic compute if the "penalty" is high enough
            phase = "MEMORY_BOUND"
        elif comm_score > comp_score:
            phase = "COMMUNICATION"
        else:
            # Default to Compute if no other distinct features are strong [cite: 299]
            phase = "COMPUTE"

        # # Stabilize to prevent rapid oscillation between phases
        # final_phase = self._stabilize_phase(phase)
        final_phase = phase

        details = {
            'phase': final_phase,
            'scores': f"Comp:{comp_score:.1f} Comm:{comm_score:.1f} Mem:{mem_score:.1f}",
            'reasons': ', '.join(reasons)
        }
        return final_phase, details


class FrequencyController:
    def __init__(self):
        self.handles = []
        
        # Find all CPU governor files
        gov_files = glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")
        
        if not gov_files:
            print("[WARN] No CPU frequency scaling paths found.")
            return

        success_count = 0
        for gov in gov_files:
            try:
                # Open in text mode (default buffering)
                f = open(gov, 'w')
                self.handles.append(f)
                success_count += 1
            except PermissionError:
                # Mimic V16 behavior: Ignore errors if we aren't root
                pass
            except OSError:
                pass
        
        # If we couldn't open any files, warn the user but DON'T CRASH
        if success_count == 0:
            print("[WARN] No root access for DVFS. Running in MONITOR-ONLY mode.")
        else:
            print(f"[INFO] Frequency control enabled on {success_count} cores.")

    def set_mode(self, mode: str):
        # If no handles (because no sudo), this function does nothing safely
        if not self.handles: 
            return
        
        target = "performance" if mode == "COMPUTE" else "powersave"
        for h in self.handles:
            try:
                h.seek(0)
                h.write(target)
                h.flush() # Force write to apply setting immediately
            except OSError:
                pass

class IntelligentMonitorv17:
    def __init__(self, args):
        self.config = {
            'rapl_path': "/sys/class/powercap/intel-rapl:0/energy_uj",
            'perf_bin': PERF
        }

        self.collector = MetricsCollector(self.config)
        self.analyzer = PhaseAnalyzer(expected_ranks=args.ranks)
        self.controller = FrequencyController()


        # Setup CSV
        self.filename = f"monitor_new.csv"
        self.csv_file = open(self.filename, 'w', newline='')
        # # We need to define headers based on SystemMetrics + Phase Details
        fieldnames = [
            'timestamp', 'phase', 'ipc', 'miss_rate', 'pkg_power', 'dram_power',
            'net_rx', 'net_tx', 'cpu_util_eff', 'iowait', 'ctx_switches', 'sync_var', # <--- Added 'iowait'
            'scores', 'reasons'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()

    
    def get_miniMD_pids(self, existing_pids=None):
        if existing_pids and len(existing_pids) > 0:
            try:
                os.kill(existing_pids[0], 0)
                return existing_pids 
            except OSError: pass 

        try:
            # Look specifically for the binary name
            result = subprocess.run(["pgrep", "miniMD_openmpi"], capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout.strip():
                pids = [int(pid) for pid in result.stdout.strip().split()]
                # filter out our own script just in case
                my_pid = os.getpid()
                return [p for p in pids if p != my_pid]
        except: pass
        return []
    
    # MAIN RUNING LOGIC
    def run(self):
        print(f"[INFO] V17 Monitoring started.")
        print("[INFO] Waiting for miniMD to run...")
        
        pids = []
        while not pids:
            pids = self.get_miniMD_pids()
            time.sleep(0.5)
        
        print(f"[INFO] Attached to {len(pids)} ranks.")
        
        last_heartbeat_time = 0
        
        try:
            while True:
                # --- START INTERVAL TIMER ---
                loop_start = time.time()
                
                # 1. Check process health
                pids = self.get_miniMD_pids(pids)
                if not pids:
                    print("[INFO] miniMD finished. Exiting.")
                    break

                # 2. Collect
                metrics = self.collector.sample(pids)

                # 3. Classify
                phase, details = self.analyzer.detect_phase(metrics)

                # 4. Act
                # self.controller.set_mode(phase)

                # 5. Log
                row = {
                    'timestamp': f"{metrics.timestamp:.4f}",
                    'phase': phase,
                    'ipc': f"{metrics.ipc:.2f}",
                    'miss_rate': f"{metrics.miss_rate:.2f}",
                    'pkg_power': f"{metrics.pkg_power:.1f}",
                    'dram_power': f"{metrics.dram_power:.1f}",
                    'net_rx': f"{metrics.net_rx_mbps:.1f}",
                    'net_tx': f"{metrics.net_tx_mbps:.1f}",
                    'cpu_util_eff': f"{metrics.effective_cpu_util:.1f}",
                    'iowait': f"{metrics.iowait_pct:.1f}",
                    'ctx_switches': f"{metrics.ctx_switches:.0f}",
                    'sync_var': f"{metrics.sync_variance:.1f}",
                    'scores': details.get('scores', ''),
                    'reasons': details.get('reasons', '')
                }
                self.writer.writerow(row)
                

                # --- 5-SECOND HEARTBEAT ---
                if loop_start - last_heartbeat_time >= 1.0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Phase: {phase:12s} | "
                          f"IPC: {metrics.ipc:.2f} | "
                          f"Pwr: {metrics.pkg_power:.0f}W | "
                          f"Eff-Util: {metrics.effective_cpu_util:.0f}%")
                    last_heartbeat_time = loop_start

                # --- DYNAMIC SLEEP ---
                # Sleep only the remainder of the interval
                elapsed = time.time() - loop_start
                sleep_time = max(0, SAMPLE_INTERVAL - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\n[STOP] Monitoring stopped by user.")
        finally:
            if self.csv_file:     
                self.csv_file.flush()           
                self.csv_file.close()
            
def main():
    import argparse
    
    # 1. Setup Argument Parser
    parser = argparse.ArgumentParser(description='INTELLIGENT MiniMD Monitor v17')
    parser.add_argument('-n', '--ranks', type=int, default=30, 
                        help='Number of MPI ranks (default: 30)')
    parser.add_argument('-c', '--command', type=str, 
                        help='Command to launch miniMD (optional override)')
    
    args = parser.parse_args()

    # 2. Handle Command Override (Global CMD variable)
    # If the user provides a specific command string, we split it for subprocess
    # if args.command:
    #     import shlex
    #     global CMD
    #     CMD = shlex.split(args.command)
    #     print(f"[CONFIG] Overriding command: {CMD}")

    # 3. Instantiate the Monitor
    # We pass 'args' because IntelligentMonitorv17 expects it in __init__
    monitor = IntelligentMonitorv17(args)
    
    # 4. Run the Main Loop
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("\n[STOP] Monitoring interrupted.")
    except Exception as e:
        print(f"[FATAL] Monitor crashed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
