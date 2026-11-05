#!/usr/bin/env python3
import os
import sys
import time
import struct
import argparse
import subprocess
import signal
import threading
import ctypes
from ctypes import c_int, c_uint16, c_uint32, c_uint64, c_longlong

# ================= CONFIGURATION =================
APP_NAME = "miniMD"
LOOP_SLEEP = 0.5

# Frequency Values (AMD EPYC 7551P)
FREQ_MAX = 2000000
FREQ_MID = 1600000
FREQ_MIN = 1200000

# Thresholds
GLOBAL_IO_THRESHOLD_MB = 15.0 # Checkpointing
UTIL_IDLE_THRESHOLD = 10.0    # MPI Waits
IPC_MEM_BOUND = 0.8           # < 0.8 means stalled on RAM (Target MID freq)
IPC_MIXED = 1.0               # < 1.0 means mixed (Target MID freq)

# ================= LOW LEVEL PERF =================
PERF_TYPE_HARDWARE = 0
PERF_COUNT_HW_CPU_CYCLES = 0
PERF_COUNT_HW_INSTRUCTIONS = 1
__NR_perf_event_open = 298

class PerfEventAttr(ctypes.Structure):
    _fields_ = [
        ("type", c_uint32), ("size", c_uint32), ("config", c_uint64),
        ("sample_period", c_uint64), ("sample_type", c_uint64), ("read_format", c_uint64),
        ("flags", c_uint64), ("wakeup_events", c_uint32), ("bp_type", c_uint32),
        ("bp_addr", c_uint64), ("bp_len", c_uint64), ("branch_sample_type", c_uint64),
        ("sample_regs_user", c_uint64), ("sample_stack_user", c_uint32),
        ("clockid", c_int), ("sample_regs_intr", c_uint64),
        ("aux_watermark", c_uint32), ("sample_max_stack", c_uint16), ("reserved2", c_uint16)
    ]

libc = ctypes.CDLL(None)

def perf_event_open(attr, pid, cpu, group_fd, flags):
    attr.size = ctypes.sizeof(attr)
    return libc.syscall(__NR_perf_event_open, ctypes.byref(attr), c_int(pid), c_int(cpu), c_int(group_fd), c_longlong(flags))

# ================= HARDWARE MONITORS =================
class CoreUtilMonitor:
    def __init__(self, cores):
        self.cores = set(cores)
        self.prev = {}
        try: self.f = open("/proc/stat", "r")
        except: pass

    def sample(self):
        if not hasattr(self, 'f'): return {}
        self.f.seek(0)
        res = {}
        for line in self.f:
            if not line.startswith("cpu"): continue
            parts = line.split()
            if len(parts[0]) == 3: continue
            c = int(parts[0][3:])
            if c not in self.cores: continue

            idle = int(parts[4]) + int(parts[5])
            total = sum(int(x) for x in parts[1:])

            if c in self.prev:
                dt = total - self.prev[c][0]
                di = idle - self.prev[c][1]
                res[c] = (1.0 - (di/dt))*100.0 if dt > 0 else 0.0
            self.prev[c] = (total, idle)
        return res

# ================= IO DETECTOR =============================
class IODetector:
    def __init__(self):
        self.handles = {}
        self.last_wchar = {}

    def get_write_mb(self, pids, dt):
        if dt <= 0: return 0.0

        # Lazy open
        for pid in pids:
            if pid not in self.handles:
                try: self.handles[pid] = open(f"/proc/{pid}/io", "r")
                except: pass

        total_delta = 0
        
        # Iterate over open handles
        for pid in list(self.handles.keys()):
            if pid not in pids:
                try: self.handles[pid].close()
                except: pass
                del self.handles[pid]
                if pid in self.last_wchar: del self.last_wchar[pid]
                continue

            try:
                f = self.handles[pid]
                f.seek(0)
                for line in f:
                    if line.startswith("wchar:"):
                        val = int(line.split()[1])
                        if pid in self.last_wchar:
                            delta = val - self.last_wchar[pid]
                            if delta > 0: total_delta += delta
                        self.last_wchar[pid] = val
                        break
            except: pass
            
        return (total_delta / 1024 / 1024) / dt

# ================= PER-PROCESS IPC MONITOR =================
class ProcessIPC:
    def __init__(self, pid):
        self.pid = pid
        self.fd_instr = self._open(PERF_COUNT_HW_INSTRUCTIONS)
        self.fd_cycles = self._open(PERF_COUNT_HW_CPU_CYCLES)
        self.prev_i = 0
        self.prev_c = 0
        self.valid = (self.fd_instr > 0 and self.fd_cycles > 0)

    def _open(self, cfg):
        attr = PerfEventAttr()
        attr.type = PERF_TYPE_HARDWARE
        attr.config = cfg
        attr.flags = (1 << 0) | (1 << 5) | (1 << 6)
        fd = perf_event_open(attr, self.pid, -1, -1, 0)
        if fd > 0: libc.ioctl(fd, 0x2400, 0) # Enable
        return fd

    def _read(self, fd):
        if fd < 0: return 0
        try: return struct.unpack('q', os.read(fd, 8))[0]
        except: return 0

    def get_ipc(self):
        curr_i = self._read(self.fd_instr)
        curr_c = self._read(self.fd_cycles)
        di = curr_i - self.prev_i
        dc = curr_c - self.prev_c
        self.prev_i = curr_i
        self.prev_c = curr_c
        return (di / dc) if dc > 0 else 0.0

    def close(self):
        if self.fd_instr > 0: os.close(self.fd_instr)
        if self.fd_cycles > 0: os.close(self.fd_cycles)

# ================= CONTROLLER =================
class Controller:
    def __init__(self, cores):
        self.handles = {}
        self.last = {}
        for c in cores:
            try:
                with open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor", 'w') as f:
                    f.write("userspace")
                self.handles[c] = open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed", 'w')
                self.last[c] = 0
            except: pass

    def set(self, core, freq):
        if core in self.handles and self.last.get(core) != freq:
            try:
                self.handles[core].seek(0)
                self.handles[core].write(str(freq))
                self.handles[core].flush()
                self.last[core] = freq
            except: pass

    def reset(self):
        for c in self.handles: self.set(c, FREQ_MAX)
    def close(self):
        for h in self.handles.values(): h.close()

# ================= MAIN =================
stop_event = threading.Event()
def signal_handler(signum, frame): stop_event.set()
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def scan_pids(app_name, cores):
    pmap = {}
    try:
        cmd = ["pgrep", "-u", os.environ.get("USER"), "-f", app_name]
        pids = [int(x) for x in subprocess.check_output(cmd, text=True).split()]
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", 'r') as f:
                    c = int(f.read().split()[38])
                    if c in cores: pmap[c] = pid
            except: pass
    except: pass
    return pmap

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, required=True)
    parser.add_argument('--heartbeat', action='store_true')
    args = parser.parse_args()

    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e+1))
        else: cores.append(int(part))
    cores.sort()

    print(f"[*] Monitor Started. Cores: {cores}")

    ctl = Controller(cores)
    io_det = IODetector()
    util = CoreUtilMonitor(cores)
    ipc_mons = {} 

    # Initial scan
    pmap = scan_pids(APP_NAME, cores)
    for c, pid in pmap.items():
        ipc_mons[c] = ProcessIPC(pid)

    last_time = time.time()
    util.sample() # Prime

    try:
        while not stop_event.is_set():
            time.sleep(LOOP_SLEEP)
            now = time.time()
            dt = now - last_time
            last_time = now

            if len(ipc_mons) < len(cores):
                pmap = scan_pids(APP_NAME, cores)
                for c, pid in pmap.items():
                    if c not in ipc_mons: ipc_mons[c] = ProcessIPC(pid)

            # 1. Gather Metrics
            curr_pids = list(pmap.values())
            
            # --- FIX APPLIED HERE ---
            io_mb = io_det.get_write_mb(curr_pids, dt) 
            # ------------------------
            
            u_data = util.sample()

            # 2. Decision
            hb = []
            io_override = (io_mb > GLOBAL_IO_THRESHOLD_MB)

            for c in cores:
                u = u_data.get(c, 0)
                ipc = 0.0

                if c in ipc_mons:
                    if ipc_mons[c].valid:
                        ipc = ipc_mons[c].get_ipc()
                    else:
                        del ipc_mons[c]

                # --- THE TRIPLE THREAT LOGIC ---
                if io_override:
                    state, target = "G_IO", FREQ_MIN

                elif u < UTIL_IDLE_THRESHOLD:
                    # OPTIMIZATION: Use MIN for Idle
                    state, target = "IDLE", FREQ_MIN 
                elif u <= 1:
                    state, target = "INACTIVE", FREQ_MIN

                elif ipc < IPC_MEM_BOUND:
                    state, target = "MEM ", FREQ_MID

                elif ipc < IPC_MIXED:
                    state, target = "MIX ", FREQ_MID

                else:
                    state, target = "COMP", FREQ_MAX

                ctl.set(c, target)

                if args.heartbeat:
                    hb.append(f"C{c}:[{state}|{int(u)}%|{ipc:.2f}]")

            if args.heartbeat:
                print(f"|IO:{io_mb:.1f}MB| " + " ".join(hb))
                sys.stdout.flush()

    except Exception as e:
        print(e)
    finally:
        print("\n[*] Stopping...")
        ctl.reset()
        ctl.close()
        for m in ipc_mons.values(): m.close()

if __name__ == "__main__":
    main()
