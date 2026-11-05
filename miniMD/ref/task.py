#!/usr/bin/env python3
import os
import time
import csv
import struct
import ctypes
import subprocess
from pathlib import Path
from ctypes import c_int, c_uint32, c_uint64, Structure, byref
from typing import List, Dict
from dataclasses import dataclass
import math
import argparse

# ==============================================================================
# SECTION 1: I/O DETECTION (Process Scanning Logic)
# ==============================================================================

APP_PGREP_PATTERN = "miniMD_openmpi"
WCHAR_THRESHOLD_BPS = 0.5 * 1024 * 1024  # 500 KB/s
IGNORE_IO_FIRST_N_SEC = 5.0
MIN_CHECKPOINT_DURATION_SEC = 30.0
CHECKPOINT_IO_TIMEOUT_SEC = 2.0

class IODetector:
    def __init__(self):
        self.last_wchar: Dict[int, int] = {}
        self.t0 = time.time()
        self.in_checkpoint = False
        self.checkpoint_start_time = None
        self.last_io_time = None
        self.current_pids = []

    def _get_pids(self) -> List[int]:
        try:
            # Only match user's own processes to avoid permission issues
            out = subprocess.check_output(
                ["pgrep", "-u", os.environ.get("USER"), "-f", APP_PGREP_PATTERN],
                text=True
            ).strip()
            if not out: return []
            return sorted({int(x) for x in out.split() if x.isdigit()})
        except subprocess.CalledProcessError:
            return []

    def _read_proc_io(self, pid: int) -> int:
        try:
            # Fallback for systems where strict permissions might block full IO read
            with open(f"/proc/{pid}/io", "r") as f:
                for line in f:
                    if line.startswith("wchar:"):
                        return int(line.split()[1])
        except (FileNotFoundError, PermissionError):
            return 0
        return 0

    def check_phase(self, dt: float):
        now = time.time()
        self.current_pids = self._get_pids()
        
        # Refresh baseline
        for pid in self.current_pids:
            if pid not in self.last_wchar:
                self.last_wchar[pid] = self._read_proc_io(pid)

        total_dwchar = 0
        for pid in self.current_pids:
            cur = self._read_proc_io(pid)
            prev = self.last_wchar.get(pid, cur)
            total_dwchar += max(0, cur - prev)
            self.last_wchar[pid] = cur

        wchar_Bps = total_dwchar / max(dt, 1e-6)
        
        # Logic
        t = now - self.t0
        io_detected = (wchar_Bps >= WCHAR_THRESHOLD_BPS) and (t >= IGNORE_IO_FIRST_N_SEC)

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

        return self.in_checkpoint, self.current_pids

# ==============================================================================
# SECTION 2: PERF_EVENT_OPEN (PID-BASED MONITORING)
# ==============================================================================

libc = ctypes.CDLL(None, use_errno=True)
syscall = libc.syscall
__NR_perf_event_open = 298

PERF_TYPE_HARDWARE = 0
PERF_TYPE_HW_CACHE = 3
PERF_COUNT_HW_CPU_CYCLES = 0
PERF_COUNT_HW_INSTRUCTIONS = 1
LLC_READ_MISS_CONFIG = (2) | (0 << 8) | (1<<16)

class PerfEventAttr(Structure):
    _fields_ = [
        ("type", c_uint32), ("size", c_uint32), ("config", c_uint64),
        ("sample_period", c_uint64), ("sample_type", c_uint64), ("read_format", c_uint64),
        ("disabled", c_uint64, 1), ("inherit", c_uint64, 1), ("pinned", c_uint64, 1),
        ("exclusive", c_uint64, 1), ("exclude_user", c_uint64, 1), ("exclude_kernel", c_uint64, 1),
        ("exclude_hv", c_uint64, 1), ("exclude_idle", c_uint64, 1), ("mmap", c_uint64, 1),
        ("comm", c_uint64, 1), ("freq", c_uint64, 1), ("inherit_stat", c_uint64, 1),
        ("enable_on_exec", c_uint64, 1), ("task", c_uint64, 1), ("watermark", c_uint64, 1),
        ("precise_ip", c_uint64, 2), ("mmap_data", c_uint64, 1), ("sample_id_all", c_uint64, 1),
        ("exclude_host", c_uint64, 1), ("exclude_guest", c_uint64, 1),
        ("exclude_callchain_kernel", c_uint64, 1), ("exclude_callchain_user", c_uint64, 1),
        ("mmap2", c_uint64, 1), ("comm_exec", c_uint64, 1), ("use_clockid", c_uint64, 1),
        ("context_switch", c_uint64, 1), ("write_backward", c_uint64, 1),
        ("namespaces", c_uint64, 1), ("reserved", c_uint64, 35)
    ]

def perf_event_open(attr, pid, cpu, group_fd, flags):
    attr.size = ctypes.sizeof(attr)
    return syscall(__NR_perf_event_open, byref(attr), c_int(pid), c_int(cpu), c_int(group_fd), c_uint64(flags))

# Fixed Frequencies
FREQ_MAX = 2400000
FREQ_MID = 1600000
FREQ_MIN = 1200000

# Logic Thresholds
IPC_COMPUTE_THRESHOLD = 1.5
IPC_LOW_THRESHOLD = 0.9
MPKI_MEM_THRESHOLD = 5.0      
MPKI_SPIN_THRESHOLD = 0.5      

ALPHA = 0.3                   
CONFIDENCE_SWITCH = 3          
MIN_DWELL_TIME = 0.5           

@dataclass 
class CoreData:
    pid: int
    core_id: int
    ipc: float
    mpki: float
    util: float
    is_spinning: bool

class PidMonitor:
    """
    Monitors a specific PID.
    Compatible with perf_event_paranoid=2 because we own the process.
    """
    def __init__(self, pid):
        self.pid = pid
        self.fds = {}
        self.prev = {}
        events = [
            ('cycles', PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES),
            ('instr', PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS),
            ('llc_miss', PERF_TYPE_HW_CACHE, LLC_READ_MISS_CONFIG),
        ]
        
        for name, type_id, config_id in events:
            attr = PerfEventAttr()
            attr.type = type_id
            attr.config = config_id
            attr.disabled = 0
            attr.exclude_kernel = 1  # Crucial for paranoid=2
            attr.exclude_hv = 1
            attr.inherit = 0         # Don't trace children, just the MPI rank
            
            # pid=pid, cpu=-1 (Monitor this PID on ANY CPU)
            fd = perf_event_open(attr, pid, -1, -1, 0)
            
            if fd < 0:
                # Silent fail can happen if process finished during init
                self.fds[name] = -1
            else:
                self.fds[name] = fd
                self.prev[name] = 0

    def read(self):
        deltas = {}
        for name, fd in self.fds.items():
            if fd < 0:
                deltas[name] = 0
                continue
            try:
                raw = os.read(fd, 8)
                val = struct.unpack('Q', raw)[0]
                diff = val if val < self.prev[name] else val - self.prev[name]
                self.prev[name] = val
                deltas[name] = diff
            except OSError:
                deltas[name] = 0
        return deltas
    
    def get_current_core(self) -> int:
        """Reads /proc/<pid>/stat to find which core this PID is on"""
        try:
            with open(f"/proc/{self.pid}/stat", 'r') as f:
                # Field 38 is 'processor' (0-indexed -> index 38 is the 39th field)
                # But splitting by space can be risky if comm has spaces. 
                # Safe way: rpartition(')')[2]
                content = f.read()
                part = content.rpartition(')')[2]
                fields = part.split()
                # 'processor' is usually the 37th field after the closing parenthesis
                # or index 38 in the full list.
                # In standard kernel, it is field 39 (index 38).
                return int(fields[36]) # index 36 in the split 'rest' string?
                # Let's rely on full split, assuming simple comm name
        except:
            pass
            
        # Fallback: full split
        try:
            with open(f"/proc/{self.pid}/stat", 'r') as f:
                return int(f.read().split()[38])
        except:
            return -1

    def close(self):
        for fd in self.fds.values():
            if fd > 0: os.close(fd)

class MetricsCollector:
    def __init__(self, config):
        self.config = config
        self.monitors: Dict[int, PidMonitor] = {} # Map PID -> Monitor
        
        # Power & Net (System Wide - these usually readable)
        self.files = {
            'net':  open("/proc/net/dev", 'r'),
            'rapl': open(config['rapl_path'], 'r') if os.path.exists(config['rapl_path']) else None
        }
        self.prev = {
            'time': time.time(),
            'pkg_e': self._read_rapl(self.files.get('rapl')),
            'net_r': 0, 'net_s': 0
        }

    def update_monitors(self, current_pids: List[int]):
        """Syncs internal monitor list with detected PIDs"""
        # Add new
        for pid in current_pids:
            if pid not in self.monitors:
                self.monitors[pid] = PidMonitor(pid)
        
        # Remove dead
        dead = [pid for pid in self.monitors if pid not in current_pids]
        for pid in dead:
            self.monitors[pid].close()
            del self.monitors[pid]

    def sample(self, current_pids: List[int]):
        self.update_monitors(current_pids)
        
        now = time.time()
        dt = now - self.prev['time']
        if dt <= 0: dt = 0.05
        
        core_results = []
        total_ipc_num = 0; total_ipc_den = 0
        
        # Read Per-PID Metrics
        for pid, mon in self.monitors.items():
            d = mon.read()
            c_cyc = d.get('cycles', 0); c_ins = d.get('instr', 0)
            c_miss = d.get('llc_miss', 0)
            
            core_id = mon.get_current_core()
            
            if core_id == -1: continue # Process dead/moved
            
            c_ipc = c_ins / c_cyc if c_cyc > 0 else 0
            c_mpki = (c_miss / c_ins * 1000) if c_ins > 0 else 0
            is_spinning = (c_ipc > IPC_COMPUTE_THRESHOLD and c_mpki < MPKI_SPIN_THRESHOLD)
            
            # Using cycles as proxy for utilization since we are monitoring the PID
            core_results.append(CoreData(pid, core_id, c_ipc, c_mpki, c_cyc, is_spinning))
            
            total_ipc_num += c_ins
            total_ipc_den += c_cyc

        # System Metrics
        curr_pkg = self._read_rapl(self.files.get('rapl'))
        watts_pkg = self._safe_delta(curr_pkg, self.prev['pkg_e']) / 1e6 / dt
        
        global_ipc = total_ipc_num / total_ipc_den if total_ipc_den > 0 else 0
        
        self.prev.update({'time': now, 'pkg_e': curr_pkg})
        
        return global_ipc, watts_pkg, core_results

    def _safe_delta(self, c, p): return c - p if c >= p else (2**32 - p) + c
    def _read_rapl(self, f):
        if not f: return 0
        f.seek(0); 
        try: return int(f.read())
        except: return 0

# ==============================================================================
# SECTION 3: CONTROLLER
# ==============================================================================

class CorePhaseState:
    def __init__(self):
        self.phase = "COMPUTE"; self.confidence = 0; self.last_switch = time.time()
        self.avg_ipc = 0.0; self.avg_mpki = 0.0

class DirectFrequencyController:
    def __init__(self, allowed_cores: List[int]):
        self.allowed_cores = set(allowed_cores)
        self.handles = {}
        self.state = {c: CorePhaseState() for c in allowed_cores}
        
        # Open all possible frequency handles
        for c in allowed_cores:
            path = f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed"
            try:
                self.handles[c] = open(path, 'w')
            except OSError:
                pass # Might not have permission or file doesn't exist

    def target_frequency(self, phase: str):
        if phase in ["IDLE", "SPIN", "IO_OVERRIDE"]: return FREQ_MIN
        if phase == "MEMORY": return FREQ_MID
        return FREQ_MAX

    def update(self, core_data_list: List[CoreData], force_io_phase: bool):
        now = time.time()
        
        # core_data_list contains one entry per PID
        for data in core_data_list:
            cid = data.core_id
            
            if cid not in self.handles: continue
            
            if force_io_phase:
                self._apply_freq(cid, self.target_frequency("IO_OVERRIDE"))
                continue

            s = self.state[cid]
            s.avg_ipc = ALPHA * data.ipc + (1 - ALPHA) * s.avg_ipc
            s.avg_mpki = ALPHA * data.mpki + (1 - ALPHA) * s.avg_mpki
            
            # Classify
            observed = s.phase
            if s.avg_ipc > IPC_COMPUTE_THRESHOLD and s.avg_mpki < MPKI_SPIN_THRESHOLD: observed = "SPIN"
            elif s.avg_ipc < IPC_LOW_THRESHOLD and s.avg_mpki > MPKI_MEM_THRESHOLD: observed = "MEMORY"
            elif s.avg_ipc > IPC_COMPUTE_THRESHOLD and s.avg_mpki < MPKI_MEM_THRESHOLD: observed = "COMPUTE"
            
            # Confidence
            if observed == s.phase: s.confidence = min(6, s.confidence + 1)
            else: s.confidence = max(-6, s.confidence - 1)
            
            # Switch
            if observed != s.phase and abs(s.confidence) >= CONFIDENCE_SWITCH and (now - s.last_switch) >= MIN_DWELL_TIME:
                s.phase = observed; s.last_switch = now; s.confidence = 0
                
            self._apply_freq(cid, self.target_frequency(s.phase))

    def _apply_freq(self, core, freq):
        try:
            self.handles[core].seek(0)
            self.handles[core].write(str(freq))
            self.handles[core].flush()
        except OSError: pass

    def close(self):
        for h in self.handles.values(): h.close()

class NativeMonitor:
    def __init__(self, cores):
        self.config = {'rapl_path': "/sys/class/powercap/intel-rapl:0/energy_uj"}
        self.collector = MetricsCollector(self.config)
        self.controller = DirectFrequencyController(cores)
        self.io_detector = IODetector()
        
        self.csv_file = open("monitor_log.csv", 'w', newline='')
        self.writer = csv.writer(self.csv_file)
        self.writer.writerow(['timestamp', 'phase', 'ipc', 'pkg_pwr', 'active_pids', 'io_detected'])
        
    def run(self):
        print("[INFO] PID-Based Monitor Started. Waiting for MiniMD...")
        try:
            while True:
                time.sleep(0.2)
                
                # 1. IO Detection (Finds PIDs too)
                is_io_phase, current_pids = self.io_detector.check_phase(0.05)
                
                if not current_pids:
                    # No MiniMD running yet?
                    continue
                
                # 2. Sample Metrics
                ipc, pwr, core_data = self.collector.sample(current_pids)
                
                # 3. Control
                self.controller.update(core_data, force_io_phase=is_io_phase)
                
                # 4. Log
                phase_label = "IO_DISK" if is_io_phase else "COMPUTE"
                self.writer.writerow([f"{time.time():.4f}", phase_label, f"{ipc:.2f}", f"{pwr:.1f}", len(current_pids), 1 if is_io_phase else 0])
                
        except KeyboardInterrupt:
            print("\n[STOP] Stopping...")
        finally:
            self.controller.close()
            self.csv_file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, default="0-7")
    args = parser.parse_args()
    
    # Parse core string
    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e+1))
        else:
            cores.append(int(part))
            
    NativeMonitor(cores).run()
