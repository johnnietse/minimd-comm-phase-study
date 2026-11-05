#!/usr/bin/env python3
import os
import sys
import time
import struct
import argparse
import subprocess
import signal
import threading
import math
import fcntl

# ================= CONFIGURATION =================
APP_NAME = "miniMD"
LOOP_SLEEP = 0.5         # interval length (I)
PWM_RESOLUTION = 0.01    # duty-cycle resolution

# perf binary (can override via PERF_BIN env var)
PERF_BIN = os.environ.get("PERF_BIN", "perf")

# Available Discrete Frequencies (Sorted)
FREQ_AVAIL = [1200000, 1600000, 2000000]
FREQ_MIN = 1200000
FREQ_MID = 1600000
FREQ_MAX = 2000000

# Thresholds
GLOBAL_IO_THRESHOLD_MB = 15.0
UTIL_IDLE_THRESHOLD = 10.0
IPC_MEM_BOUND = 0.8
IPC_MIXED = 1.0

# Beta Config
SLOWDOWN_LIMIT = 0.05  # delta in paper (5%)

# ================= HARDWARE MONITORS =================
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

# ================= IPC MONITOR (perf stat -p) =================
class ProcessIPC:
    """
    Uses: perf stat -I 500 -e cycles:u,instructions:u,cache-misses:u -p <pid> -x ,
    Parses interval lines to get per-interval deltas.

    Notes:
      - perf stat writes stats to stderr by default; we read stderr.
      - We set O_NONBLOCK and drain each loop.
      - We compute per-interval (di, dc) from perf's per-interval counts.
    """
    def __init__(self, pid, interval_ms=500):
        self.pid = int(pid)
        self.interval_ms = int(interval_ms)
        self.process = None
        self.valid = False

        # latest interval deltas
        self._last_instr = 0.0
        self._last_cycles = 0.0
        self._last_misses = 0.0
        self._have_cycles = False
        self._have_instr = False

        self._start()

    def _start(self):
        # -I prints every interval; -x , makes CSV-ish output
        cmd = [
            PERF_BIN, "stat",
            "-I", str(self.interval_ms),
            "-e", "cycles:u,instructions:u,cache-misses:u",
            "-p", str(self.pid),
            "-x", ","
        ]
        try:
            self.process = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                text=True,
                bufsize=1
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
        try:
            if self.process.stderr:
                self.process.stderr.close()
        except Exception:
            pass
        self.process = None
        self.valid = False

    def _parse_perf_line(self, line):
        """
        perf -x , interval line often like:
          <time_ms>,<count>,<unit>,<event>,<...>

        We only need count + event.
        Some lines can be empty, warnings, headers, or '<not counted>'.
        """
        if not line:
            return
        s = line.strip()
        if not s or s.startswith("#"):
            return

        parts = s.split(",")
        if len(parts) < 4:
            return

        # parts[1] = count, parts[3] = event
        count_s = parts[1].strip()
        event = parts[3].strip()

        # ignore multiplex / missing
        try:
            val = float(count_s)
        except Exception:
            return

        if event == "cycles:u":
            self._last_cycles = val
            self._have_cycles = True
        elif event == "instructions:u":
            self._last_instr = val
            self._have_instr = True
        elif event == "cache-misses:u":
            self._last_misses = val

    def _drain(self):
        """
        Drain all available stderr without blocking.
        Keep only the most recent interval values for each event.
        """
        if not self.valid or self.process is None or self.process.stderr is None:
            return

        try:
            while True:
                line = self.process.stderr.readline()
                if not line:
                    break
                self._parse_perf_line(line)
        except Exception:
            # Nonblocking read can raise; ignore
            pass

        # If perf died, mark invalid
        try:
            if self.process.poll() is not None:
                self.valid = False
        except Exception:
            self.valid = False

    def get_metrics(self):
        """
        Returns: (ipc, di)
          ipc = instructions/cycles for most recent interval observed by perf
          di  = instructions in that interval (used for MIPS calc)
        """
        if not self.valid:
            return 0.0, 0

        self._drain()

        # If we haven't seen both events yet, return zeros
        if not (self._have_cycles and self._have_instr):
            return 0.0, 0

        di = self._last_instr
        dc = self._last_cycles
        ipc = (di / dc) if dc > 0 else 0.0

        # cast to int for downstream compatibility (was int delta before)
        return float(ipc), int(di)

    def close(self):
        self._stop()

# ================= BETA OPTIMIZER =================
class BetaOptimizer:
    def __init__(self):
        self.mips_table = {f: 0.0 for f in FREQ_AVAIL}
        self.beta = 1.0

    def update_clean_sample(self, freq, mips):
        """Update table only when the *entire interval* ran at a single freq."""
        if freq not in self.mips_table or mips <= 0:
            return
        self.mips_table[freq] = mips

        mips_max = self.mips_table[FREQ_MAX]
        mips_mid = self.mips_table[FREQ_MID]

        if mips_max > 0 and mips_mid > 0:
            # Power law: mips ∝ f^beta  => beta = ln(mips_max/mips_mid)/ln(f_max/f_mid)
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
                self.last[c] = None
            except Exception:
                pass

    def set(self, core, freq):
        if core in self.handles and self.last.get(core) != freq:
            try:
                self.handles[core].seek(0)
                self.handles[core].write(str(int(freq)))
                self.handles[core].flush()
                self.last[core] = freq
            except Exception:
                pass

    def reset(self):
        for c in self.handles:
            self.set(c, FREQ_MAX)

    def close(self):
        for h in self.handles.values():
            try:
                h.close()
            except Exception:
                pass

# ================= MAIN =================
stop_event = threading.Event()
def signal_handler(signum, frame):
    stop_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def scan_pids(app_name, cores):
    """
    Returns mapping core->pid by reading /proc/<pid>/stat 'processor' field (index 38).
    """
    cores_set = set(cores)
    pmap = {}
    try:
        cmd = ["pgrep", "-u", os.environ.get("USER"), "-f", app_name]
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, required=True)
    parser.add_argument('--heartbeat', action='store_true')
    args = parser.parse_args()

    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e + 1))
        else:
            cores.append(int(part))
    cores = sorted(set(cores))

    print(f"[*] Beta-Integrated PWM Emulator Started. Cores: {cores}")
    print(f"[*] Using perf binary: {PERF_BIN}")

    ctl = Controller(cores)
    io_det = IODetector()
    util = CoreUtilMonitor(cores)

    ipc_mons = {}              # core -> ProcessIPC
    betas = {c: BetaOptimizer() for c in cores}

    # prev_plan is what was actually applied during the *previous* interval
    # core -> (f_low, f_high, r, t_low)
    prev_plan = {c: (FREQ_MAX, FREQ_MAX, 1.0, LOOP_SLEEP) for c in cores}

    # calibration state per core:
    # 0 = need MAX sample, 1 = need MID sample, 2 = ready for beta PWM
    cal_state = {c: 0 for c in cores}

    # Prime monitors
    util.sample()
    last_loop_time = time.time()

    try:
        while not stop_event.is_set():
            now = time.time()
            dt = now - last_loop_time
            last_loop_time = now
            if dt <= 0:
                dt = 0.001

            # Refresh PID map every loop (robust)
            pmap = scan_pids(APP_NAME, cores)

            # Keep monitors aligned with current pids; close/remove stale ones
            for c in list(ipc_mons.keys()):
                if c not in pmap:
                    ipc_mons[c].close()
                    del ipc_mons[c]
                else:
                    # If PID changed on same core, replace monitor
                    if ipc_mons[c].pid != pmap[c]:
                        ipc_mons[c].close()
                        ipc_mons[c] = ProcessIPC(pmap[c], interval_ms=int(LOOP_SLEEP * 1000))

            # Add new monitors
            for c, pid in pmap.items():
                if c not in ipc_mons:
                    ipc_mons[c] = ProcessIPC(pid, interval_ms=int(LOOP_SLEEP * 1000))

            # Gather global metrics
            curr_pids = list(pmap.values())
            io_mb = io_det.get_write_mb(curr_pids, dt)
            u_data = util.sample()
            io_override = (io_mb > GLOBAL_IO_THRESHOLD_MB)

            # Per-core: read IPC + Δinstr and update beta using PREVIOUS plan (correct labeling)
            # NOTE: update beta only on clean intervals (prev f_low==f_high)
            per_core = {}
            for c in cores:
                u = u_data.get(c, 0.0)
                ipc = 0.0
                instr_delta = 0

                if c in ipc_mons and ipc_mons[c].valid:
                    ipc, instr_delta = ipc_mons[c].get_metrics()

                # update beta using the plan that produced this interval's instr_delta
                f_prev_low, f_prev_high, r_prev, t_prev_low = prev_plan[c]
                mips = ((instr_delta / 1e6) / dt) if (dt > 0 and instr_delta > 0) else 0.0

                if f_prev_low == f_prev_high and mips > 0:
                    betas[c].update_clean_sample(f_prev_low, mips)

                # Advance calibration state when samples exist
                if cal_state[c] == 0 and betas[c].mips_table[FREQ_MAX] > 0:
                    cal_state[c] = 1
                if cal_state[c] == 1 and betas[c].mips_table[FREQ_MID] > 0:
                    cal_state[c] = 2

                per_core[c] = {
                    "u": u,
                    "ipc": ipc,
                    "di": instr_delta,
                    "mips": mips
                }

            # Decide next interval's PWM plan
            pwm_execution_map = {}
            hb = []

            for c in cores:
                u = per_core[c]["u"]
                ipc = per_core[c]["ipc"]
                instr_delta = per_core[c]["di"]
                mips = per_core[c]["mips"]

                # defaults
                f_low = FREQ_MIN
                f_high = FREQ_MIN
                r = 1.0
                state = "INIT"
                f_star_disp = 0.0

                # global overrides first
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
                    # === CALIBRATION FIRST ===
                    if cal_state[c] == 0:
                        state = "CAL_MAX"
                        f_low = f_high = FREQ_MAX
                        r = 1.0
                    elif cal_state[c] == 1:
                        state = "CAL_MID"
                        f_low = f_high = FREQ_MID
                        r = 1.0
                    else:
                        # === BETA OPTIMIZATION ===
                        f_star = betas[c].get_f_star()
                        f_star_disp = f_star
                        f_j, f_next, r_calc = betas[c].get_pwm_params(f_star)
                        f_low, f_high, r = f_j, f_next, r_calc
                        state = f"B{betas[c].beta:.2f}"

                t_low = r * LOOP_SLEEP
                pwm_execution_map[c] = (f_low, f_high, t_low)

                # record the plan we are about to apply (used next loop to label deltas)
                prev_plan[c] = (f_low, f_high, r, t_low)

                if args.heartbeat:
                    f_star_str = f"{f_star_disp/1e6:.2f}G" if f_star_disp > 0 else "---"
                    hb.append(
                        f"C{c}:[{state}"
                        f"|U:{u:4.0f}%"
                        f"|IPC:{ipc:4.2f}"
                        f"|dI:{instr_delta}"
                        f"|M:{mips:6.1f}"
                        f"|T:{f_star_str}"
                        f"|r:{r:4.2f}"
                        f"|{int(f_low/1000)}/{int(f_high/1000)}]"
                    )

            if args.heartbeat:
                print(f"|dt:{dt:.3f}s|IO:{io_mb:.1f}MB| " + " ".join(hb))
                sys.stdout.flush()

            # Actuate for one full interval
            emulate_pwm(ctl, pwm_execution_map, LOOP_SLEEP)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[*] Stopping...")
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

if __name__ == "__main__":
    main()
