#!/usr/bin/env python3
"""
Integrated Phase-Aware Frequency Controller for miniMD
=======================================================
Combines:
  - Johnnie's communication phase frequency control (phase_marker.txt)
  - Zane's beta-adaptation algorithm for compute phases
  - Gia's I/O phase (low frequency)

Core Layout (32-core node):
  Core 31:    RESERVED  — HPC maintenance, NO permission to change freq
  Core 30:    MONITOR   — this controller runs here (pinned via taskset)
  Cores 0–N:  WORKERS   — MPI processes, 1:1 core-binding

  Valid worker counts: 1, 2, 4, 8, 16, 30
  (30 = max because core 30 = monitor, core 31 = reserved)
  (16 = max power of 2)

Core-Binding Assumption:
  - 1 MPI process is bound to 1 CPU core
  - During communication: ONLY rank 0's core (core 0) does networking
  - ALL other cores/MPI processes are blocked at MPI_Barrier (idle)
  - This lets us drop frequency on idle cores to save power

Phase Strategy:
  COMPUTE phase  -> Run Zane's beta-adaptation (PWM between frequencies)
  IO phase       -> All worker cores at 1.2 GHz
  COMM phase     -> Core 0 at 2.0 GHz, all others at 1.2 GHz

Usage:
  # 16 workers (cores 0-15), controller on core 30:
  taskset -c 30 python3 integrated_freq_controller.py --workers 16 --heartbeat

  # 8 workers:
  taskset -c 30 python3 integrated_freq_controller.py --workers 8 --heartbeat

  # 30 workers (max):
  taskset -c 30 python3 integrated_freq_controller.py --workers 30 --heartbeat

  # Custom core range (advanced):
  taskset -c 30 python3 integrated_freq_controller.py --cores 0-15 --heartbeat

  # Dry-run mode:
  python3 integrated_freq_controller.py --workers 16 --dry-run --heartbeat
"""

import os
import sys
import time
import math
import struct
import argparse
import subprocess
import signal
import threading

# ================= CONFIGURATION =================
APP_NAME = "miniMD"
LOOP_SLEEP = 0.5           # interval length (I) — matches Zane's betacode.py
PWM_RESOLUTION = 0.01      # duty-cycle resolution
PHASE_MARKER = "phase_marker.txt"
PHASE_POLL_INTERVAL = 0.05 # 50ms for phase marker polling

# Core constraints
RESERVED_CORE = 31  # HPC maintenance — NO permission to change freq
MONITOR_CORE = 30   # Dedicated for this controller
VALID_WORKER_COUNTS = [1, 2, 4, 8, 16, 30]

# perf binary
PERF_BIN = os.environ.get("PERF_BIN", "perf")

# Available discrete frequencies (sorted)
FREQ_AVAIL = [1200000, 1600000, 2400000]
FREQ_MIN = 1200000
FREQ_MID = 1600000
FREQ_MAX = 2400000  # frnt115 actual max

# Thresholds (from Zane's betacode.py)
GLOBAL_IO_THRESHOLD_MB = 15.0
UTIL_IDLE_THRESHOLD = 10.0
IPC_MEM_BOUND = 0.8
IPC_MIXED = 1.0

# Beta config
SLOWDOWN_LIMIT = 0.05  # delta in paper (5% max slowdown)

# ================= HARDWARE MONITORS =================
# (Identical to Zane's CoreUtilMonitor for compatibility)

class CoreUtilMonitor:
    def __init__(self, cores):
        self.cores = set(cores)
        self.prev = {}
        try:
            self.f = open("/proc/stat", "r")
        except Exception:
            self.f = None

    def sample(self):
        if self.f is None:
            return {}
        self.f.seek(0)
        res = {}
        for line in self.f:
            if not line.startswith("cpu"):
                continue
            parts = line.split()
            if len(parts[0]) == 3:  # "cpu" aggregate
                continue
            c = int(parts[0][3:])
            if c not in self.cores:
                continue
            idle = int(parts[4]) + int(parts[5])
            total = sum(int(x) for x in parts[1:])
            if c in self.prev:
                dt = total - self.prev[c][0]
                di = idle - self.prev[c][1]
                res[c] = (1.0 - (di / dt)) * 100.0 if dt > 0 else 0.0
            self.prev[c] = (total, idle)
        return res


class IODetector:
    def __init__(self):
        self.handles = {}
        self.last_wchar = {}

    def get_write_mb(self, pids, dt):
        if dt <= 0:
            return 0.0
        for pid in pids:
            if pid not in self.handles:
                try:
                    self.handles[pid] = open(f"/proc/{pid}/io", "r")
                except Exception:
                    pass

        total_delta = 0
        for pid in list(self.handles.keys()):
            if pid not in pids:
                try:
                    self.handles[pid].close()
                except Exception:
                    pass
                self.handles.pop(pid, None)
                self.last_wchar.pop(pid, None)
                continue
            try:
                f = self.handles[pid]
                f.seek(0)
                for line in f:
                    if line.startswith("wchar:"):
                        val = int(line.split()[1])
                        if pid in self.last_wchar:
                            delta = val - self.last_wchar[pid]
                            if delta > 0:
                                total_delta += delta
                        self.last_wchar[pid] = val
                        break
            except Exception:
                pass

        return (total_delta / 1024 / 1024) / dt


# ================= BETA OPTIMIZER (from Zane's betacode.py) =================

class BetaOptimizer:
    """
    β-adaptation algorithm from the research paper:
      β = ln(mips_max / mips_mid) / ln(f_max / f_mid)
      f* = f_max / (1 + δ/β)
    Then PWM emulation between the two nearest discrete frequencies.
    """
    def __init__(self):
        self.mips_table = {f: 0.0 for f in FREQ_AVAIL}
        self.beta = 1.0

    def update_clean_sample(self, freq, mips):
        """Update table only when the entire interval ran at a single freq."""
        if freq not in self.mips_table or mips <= 0:
            return
        self.mips_table[freq] = mips

        mips_max = self.mips_table[FREQ_MAX]
        mips_mid = self.mips_table[FREQ_MID]

        if mips_max > 0 and mips_mid > 0:
            denom = math.log(FREQ_MAX / FREQ_MID)
            if abs(denom) > 1e-12:
                b = math.log(mips_max / mips_mid) / denom
                self.beta = max(0.01, min(2.0, b))

    def ready(self):
        return (self.mips_table[FREQ_MAX] > 0 and self.mips_table[FREQ_MID] > 0)

    def get_f_star(self):
        delta = SLOWDOWN_LIMIT
        return FREQ_MAX / (1.0 + delta / self.beta)

    def get_pwm_params(self, f_star):
        f_star = max(FREQ_AVAIL[0], min(FREQ_AVAIL[-1], f_star))

        f_j = FREQ_AVAIL[0]
        f_next = FREQ_AVAIL[-1]
        for i in range(len(FREQ_AVAIL) - 1):
            if FREQ_AVAIL[i] <= f_star <= FREQ_AVAIL[i + 1]:
                f_j = FREQ_AVAIL[i]
                f_next = FREQ_AVAIL[i + 1]
                break

        if f_j == f_next:
            return f_j, f_next, 1.0

        delta = SLOWDOWN_LIMIT
        term_target = (1.0 + delta / self.beta) / FREQ_MAX
        term_next = 1.0 / f_next
        term_j = 1.0 / f_j

        numerator = term_target - term_next
        denominator = term_j - term_next
        if abs(denominator) < 1e-12:
            r = 1.0
        else:
            r = numerator / denominator

        r = max(0.0, min(1.0, r))
        return f_j, f_next, r


# ================= FREQUENCY CONTROLLER =================

class Controller:
    """Direct cpufreq sysfs writes (from Zane's betacode.py)
    Never touches RESERVED_CORE (31) — no permission."""
    def __init__(self, cores, dry_run=False):
        self.handles = {}
        self.last = {}
        self.dry_run = dry_run
        for c in cores:
            if c == RESERVED_CORE:
                continue  # Never touch core 31
            if dry_run:
                self.handles[c] = None
                self.last[c] = None
            else:
                try:
                    with open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor", 'w') as f:
                        f.write("userspace")
                    self.handles[c] = open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed", 'w')
                    self.last[c] = None
                except Exception:
                    pass

    def set(self, core, freq):
        if self.dry_run:
            self.last[core] = freq
            return
        if core in self.handles and self.last.get(core) != freq:
            try:
                self.handles[core].seek(0)
                self.handles[core].write(str(int(freq)))
                self.handles[core].flush()
                self.last[core] = freq
            except Exception:
                pass

    def set_all(self, cores, freq):
        for c in cores:
            self.set(c, freq)

    def reset(self):
        for c in self.handles:
            self.set(c, FREQ_MAX)

    def close(self):
        if self.dry_run:
            return
        for h in self.handles.values():
            if h:
                try:
                    h.close()
                except Exception:
                    pass


# ================= PWM EMULATION =================

def emulate_pwm(ctl, pwm_map, duration):
    """
    pwm_map: { core_id: (freq_low, freq_high, time_at_low) }
    """
    start = time.time()
    while True:
        now = time.time()
        elapsed = now - start
        if elapsed >= duration:
            break

        for c, (f_low, f_high, t_low) in pwm_map.items():
            if elapsed < t_low:
                ctl.set(c, f_low)
            else:
                ctl.set(c, f_high)

        time.sleep(PWM_RESOLUTION)


# ================= PID SCANNING =================

def scan_pids(app_name, cores):
    """Returns mapping core->pid by reading /proc/<pid>/stat 'processor' field."""
    cores_set = set(cores)
    pmap = {}
    try:
        cmd = ["pgrep", "-u", os.environ.get("USER", ""), "-f", app_name]
        out = subprocess.check_output(cmd, text=True).strip()
        if not out:
            return {}
        pids = [int(x) for x in out.split()]
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", 'r') as f:
                    parts = f.read().split()
                    if len(parts) > 38:
                        c = int(parts[38])
                        if c in cores_set:
                            pmap[c] = pid
            except Exception:
                pass
    except Exception:
        pass
    return pmap


# ================= PHASE MARKER READER =================

def read_phase_marker(marker_path):
    """Read the phase marker file written by C-side miniMD."""
    try:
        if os.path.exists(marker_path):
            with open(marker_path, 'r') as f:
                content = f.read().strip()
            return content
    except Exception:
        pass
    return None


# ================= MAIN LOOP =================

stop_event = threading.Event()
def signal_handler(signum, frame):
    stop_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    parser = argparse.ArgumentParser(
        description="Integrated Phase-Aware Frequency Controller for miniMD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Core Layout (32-core node):
  Core 31:    RESERVED (HPC maintenance, no permission)
  Core 30:    MONITOR  (this controller, pin with: taskset -c 30)
  Cores 0-N:  WORKERS  (MPI processes, 1:1 core-binding)

Valid --workers values: 1, 2, 4, 8, 16, 30

Examples:
  taskset -c 30 python3 integrated_freq_controller.py --workers 16 --heartbeat
  taskset -c 30 python3 integrated_freq_controller.py --workers 8 --dry-run
  taskset -c 30 python3 integrated_freq_controller.py --cores 0-7 --heartbeat
""")
    parser.add_argument('--workers', type=int, default=None,
                        help=f"Number of worker cores/MPI ranks. Valid: {VALID_WORKER_COUNTS}. "
                             f"Shorthand for --cores 0-(N-1).")
    parser.add_argument('--cores', type=str, default=None,
                        help="Cores to manage, e.g. '0-15' or '0,1,2,3' (alternative to --workers)")
    parser.add_argument('--rank0-core', type=int, default=0,
                        help="Core assigned to MPI rank 0 (default: 0)")
    parser.add_argument('--marker', type=str, default=PHASE_MARKER,
                        help=f"Phase marker file (default: {PHASE_MARKER})")
    parser.add_argument('--heartbeat', action='store_true',
                        help="Print per-core status each interval")
    parser.add_argument('--dry-run', action='store_true',
                        help="Print actions without changing frequencies")
    parser.add_argument('--log', type=str, default=None,
                        help="CSV log file for phase transitions")
    args = parser.parse_args()

    # Parse core list — either from --workers or --cores
    if args.workers is not None and args.cores is not None:
        print("ERROR: Specify either --workers or --cores, not both.")
        sys.exit(1)
    if args.workers is None and args.cores is None:
        print("ERROR: Must specify either --workers or --cores.")
        print(f"  --workers N  (valid: {VALID_WORKER_COUNTS})")
        print(f"  --cores RANGE  (e.g. 0-15)")
        sys.exit(1)

    if args.workers is not None:
        if args.workers not in VALID_WORKER_COUNTS:
            print(f"ERROR: Invalid worker count: {args.workers}")
            print(f"Valid counts: {VALID_WORKER_COUNTS}")
            sys.exit(1)
        cores = list(range(args.workers))
    else:
        cores = []
        for part in args.cores.split(','):
            if '-' in part:
                s, e = map(int, part.split('-'))
                cores.extend(range(s, e + 1))
            else:
                cores.append(int(part))
        cores = sorted(set(cores))

    # Safety: never include reserved core 31
    if RESERVED_CORE in cores:
        print(f"WARNING: Removing core {RESERVED_CORE} from list (reserved, no permission)")
        cores.remove(RESERVED_CORE)

    rank0_core = args.rank0_core
    marker_path = args.marker

    print(f"[CTRL] Integrated Phase-Aware Frequency Controller")
    print(f"[CTRL] Workers: {len(cores)} (cores {cores[0]}-{cores[-1]})")
    print(f"[CTRL] Monitor: core {MONITOR_CORE}")
    print(f"[CTRL] Reserved: core {RESERVED_CORE} (no permission — never touched)")
    print(f"[CTRL] Rank 0 core: {rank0_core}")
    print(f"[CTRL] Marker: {marker_path}")
    print(f"[CTRL] Dry run: {args.dry_run}")
    print(f"[CTRL] Frequencies: {[f/1000 for f in FREQ_AVAIL]} MHz")
    print(f"[CTRL] Beta slowdown limit: {SLOWDOWN_LIMIT*100:.0f}%")
    print()

    # Initialize components
    ctl = Controller(cores, dry_run=args.dry_run)
    io_det = IODetector()
    util = CoreUtilMonitor(cores)
    betas = {c: BetaOptimizer() for c in cores}

    log_file = None
    if args.log:
        log_file = open(args.log, 'w')
        log_file.write("timestamp,phase,rank0_freq,other_freq,data_bytes,beta_avg\n")

    # Phase state
    current_phase = "COMPUTE"     # Start in compute mode
    phase_start = time.time()
    transition_count = 0

    # Calibration state per core (from Zane)
    cal_state = {c: 0 for c in cores}  # 0=need MAX, 1=need MID, 2=ready

    # Previous plan per core (for beta labeling)
    prev_plan = {c: (FREQ_MAX, FREQ_MAX, 1.0, LOOP_SLEEP) for c in cores}

    # IPC monitors (populated when PIDs are found)
    ipc_mons = {}

    # Prime monitors
    util.sample()
    last_loop_time = time.time()

    print("[CTRL] Starting. Waiting for miniMD processes...")
    print()

    try:
        while not stop_event.is_set():
            now = time.time()
            dt = now - last_loop_time
            last_loop_time = now
            if dt <= 0:
                dt = 0.001

            # ---- Check phase marker (from C-side) ----
            content = read_phase_marker(marker_path)
            new_phase = current_phase

            if content:
                if content.startswith("COMM_START"):
                    new_phase = "COMMUNICATION"
                elif content.startswith("COMM_END"):
                    new_phase = "COMPUTE"
                elif content.startswith("IO_START"):
                    new_phase = "IO"
                elif content.startswith("IO_END"):
                    new_phase = "COMPUTE"
                elif content.startswith("COMPUTE_RESUME"):
                    new_phase = "COMPUTE"

            # ---- Handle phase transitions ----
            if new_phase != current_phase:
                duration = now - phase_start
                transition_count += 1

                data_bytes = 0
                if content and "COMM_START" in content:
                    parts = content.split()
                    if len(parts) >= 2:
                        try:
                            data_bytes = int(parts[1])
                        except ValueError:
                            pass

                ts = time.strftime("%H:%M:%S")
                print(f"[{ts}] PHASE: {current_phase} -> {new_phase} ({duration:.3f}s)")

                if new_phase == "COMMUNICATION":
                    # RANK 0 CORE: HIGH (active networking)
                    # ALL OTHER CORES: LOW (blocked at MPI_Barrier, idle)
                    ctl.set(rank0_core, FREQ_MAX)
                    for c in cores:
                        if c != rank0_core:
                            ctl.set(c, FREQ_MIN)
                    print(f"  Core {rank0_core}: {FREQ_MAX/1e6:.1f}GHz (networking)")
                    print(f"  Others: {FREQ_MIN/1e6:.1f}GHz (blocked/idle)")
                    if data_bytes > 0:
                        print(f"  Data: {data_bytes/(1024*1024):.1f}MB")

                elif new_phase == "IO":
                    # ALL CORES LOW (I/O-bound, per Gia's optimization)
                    ctl.set_all(cores, FREQ_MIN)
                    print(f"  All cores: {FREQ_MIN/1e6:.1f}GHz (I/O-bound)")

                elif new_phase == "COMPUTE":
                    # RESTORE: all cores to MAX for beta-adaptation to take over
                    ctl.set_all(cores, FREQ_MAX)
                    # Reset calibration so beta re-adapts after phase change
                    for c in cores:
                        cal_state[c] = 0
                        betas[c] = BetaOptimizer()
                    print(f"  All cores: {FREQ_MAX/1e6:.1f}GHz (beta-adaptation starting)")

                if log_file:
                    beta_avg = sum(b.beta for b in betas.values()) / len(betas) if betas else 0
                    log_file.write(f"{now},{new_phase},{FREQ_MAX},{FREQ_MIN},{data_bytes},{beta_avg:.3f}\n")
                    log_file.flush()

                current_phase = new_phase
                phase_start = now

            # ---- During COMMUNICATION or IO: skip beta logic, just hold frequencies ----
            if current_phase in ("COMMUNICATION", "IO"):
                time.sleep(PHASE_POLL_INTERVAL)
                continue

            # ---- During COMPUTE: run Zane's beta-adaptation ----

            # Refresh PID map
            pmap = scan_pids(APP_NAME, cores)

            # Maintain IPC monitors
            for c in list(ipc_mons.keys()):
                if c not in pmap:
                    ipc_mons[c].close()
                    del ipc_mons[c]
                elif ipc_mons[c].pid != pmap[c]:
                    ipc_mons[c].close()
                    try:
                        import fcntl
                        ipc_mons[c] = ProcessIPC(pmap[c], interval_ms=int(LOOP_SLEEP * 1000))
                    except Exception:
                        pass

            for c, pid in pmap.items():
                if c not in ipc_mons:
                    try:
                        ipc_mons[c] = ProcessIPC(pid, interval_ms=int(LOOP_SLEEP * 1000))
                    except Exception:
                        pass

            # Gather metrics
            curr_pids = list(pmap.values())
            io_mb = io_det.get_write_mb(curr_pids, dt)
            u_data = util.sample()
            io_override = (io_mb > GLOBAL_IO_THRESHOLD_MB)

            # Per-core beta update and decision
            pwm_execution_map = {}
            hb = []

            for c in cores:
                u = u_data.get(c, 0.0)
                ipc = 0.0
                instr_delta = 0

                if c in ipc_mons and ipc_mons[c].valid:
                    ipc, instr_delta = ipc_mons[c].get_metrics()

                # Update beta from previous interval's plan
                f_prev_low, f_prev_high, r_prev, t_prev_low = prev_plan[c]
                mips = ((instr_delta / 1e6) / dt) if (dt > 0 and instr_delta > 0) else 0.0

                if f_prev_low == f_prev_high and mips > 0:
                    betas[c].update_clean_sample(f_prev_low, mips)

                # Advance calibration
                if cal_state[c] == 0 and betas[c].mips_table[FREQ_MAX] > 0:
                    cal_state[c] = 1
                if cal_state[c] == 1 and betas[c].mips_table[FREQ_MID] > 0:
                    cal_state[c] = 2

                # Decide frequency
                f_low = FREQ_MIN
                f_high = FREQ_MIN
                r = 1.0
                state = "INIT"
                f_star_disp = 0.0

                if io_override:
                    state = "G_IO"
                    f_low = f_high = FREQ_MIN
                    r = 1.0
                elif u < UTIL_IDLE_THRESHOLD or u <= 1:
                    state = "IDLE"
                    f_low = f_high = FREQ_MIN
                    r = 1.0
                elif ipc < IPC_MEM_BOUND:
                    state = "MEM"
                    f_low = f_high = FREQ_MID
                    r = 1.0
                elif ipc < IPC_MIXED:
                    state = "MIX"
                    f_low = f_high = FREQ_MID
                    r = 1.0
                else:
                    # Calibration → Beta optimization
                    if cal_state[c] == 0:
                        state = "CAL_MAX"
                        f_low = f_high = FREQ_MAX
                        r = 1.0
                    elif cal_state[c] == 1:
                        state = "CAL_MID"
                        f_low = f_high = FREQ_MID
                        r = 1.0
                    else:
                        f_star = betas[c].get_f_star()
                        f_star_disp = f_star
                        f_j, f_next, r_calc = betas[c].get_pwm_params(f_star)
                        f_low, f_high, r = f_j, f_next, r_calc
                        state = f"B{betas[c].beta:.2f}"

                t_low = r * LOOP_SLEEP
                pwm_execution_map[c] = (f_low, f_high, t_low)
                prev_plan[c] = (f_low, f_high, r, t_low)

                if args.heartbeat:
                    f_star_str = f"{f_star_disp/1e6:.2f}G" if f_star_disp > 0 else "---"
                    hb.append(
                        f"C{c}:[{state}"
                        f"|U:{u:4.0f}%"
                        f"|IPC:{ipc:4.2f}"
                        f"|M:{mips:6.1f}"
                        f"|T:{f_star_str}"
                        f"|r:{r:4.2f}"
                        f"|{int(f_low/1000)}/{int(f_high/1000)}]"
                    )

            if args.heartbeat and len(hb) > 0:
                print(f"|dt:{dt:.3f}s|IO:{io_mb:.1f}MB|PH:{current_phase}| " + " ".join(hb))
                sys.stdout.flush()

            # Actuate PWM for one full interval
            emulate_pwm(ctl, pwm_execution_map, LOOP_SLEEP)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"\n[CTRL] Stopping. Total transitions: {transition_count}")
        try:
            ctl.reset()
            ctl.close()
        except Exception:
            pass
        for m in ipc_mons.values():
            try:
                m.close()
            except Exception:
                pass
        if log_file:
            log_file.close()


# ================= ProcessIPC (from Zane's betacode.py) =================
# Requires fcntl (Linux only) — imported inside to avoid crash on Windows

class ProcessIPC:
    """Uses perf stat -I to get per-interval IPC and instruction count."""
    def __init__(self, pid, interval_ms=500):
        import fcntl
        self.pid = int(pid)
        self.interval_ms = int(interval_ms)
        self.process = None
        self.valid = False
        self._last_instr = 0.0
        self._last_cycles = 0.0
        self._have_cycles = False
        self._have_instr = False
        self._start()

    def _start(self):
        import fcntl
        cmd = [
            PERF_BIN, "stat",
            "-I", str(self.interval_ms),
            "-e", "cycles:u,instructions:u,cache-misses:u",
            "-p", str(self.pid),
            "-x", ","
        ]
        try:
            self.process = subprocess.Popen(
                cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL,
                text=True, bufsize=1
            )
            fd = self.process.stderr.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
            self.valid = True
        except Exception:
            self.process = None
            self.valid = False

    def _stop(self):
        if self.process is None:
            return
        try:
            self.process.terminate()
        except Exception:
            pass
        try:
            self.process.kill()
        except Exception:
            pass
        self.process = None
        self.valid = False

    def _drain(self):
        if not self.valid or self.process is None or self.process.stderr is None:
            return
        try:
            while True:
                line = self.process.stderr.readline()
                if not line:
                    break
                self._parse_line(line)
        except Exception:
            pass
        try:
            if self.process.poll() is not None:
                self.valid = False
        except Exception:
            self.valid = False

    def _parse_line(self, line):
        if not line:
            return
        s = line.strip()
        if not s or s.startswith("#"):
            return
        parts = s.split(",")
        if len(parts) < 4:
            return
        try:
            val = float(parts[1].strip())
        except Exception:
            return
        event = parts[3].strip()
        if event == "cycles:u":
            self._last_cycles = val
            self._have_cycles = True
        elif event == "instructions:u":
            self._last_instr = val
            self._have_instr = True

    def get_metrics(self):
        if not self.valid:
            return 0.0, 0
        self._drain()
        if not (self._have_cycles and self._have_instr):
            return 0.0, 0
        di = self._last_instr
        dc = self._last_cycles
        ipc = (di / dc) if dc > 0 else 0.0
        return float(ipc), int(di)

    def close(self):
        self._stop()


if __name__ == "__main__":
    main()
