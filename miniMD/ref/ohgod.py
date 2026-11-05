#!/usr/bin/env python3
import os
import sys
import time
import argparse
import subprocess
import signal
import threading
import fcntl
from collections import deque

# ================= CONFIGURATION =================
APP_NAME = "miniMD"
LOOP_SLEEP = 0.5         # interval length (I)
PWM_RESOLUTION = 0.01    # duty-cycle resolution

# perf binary (can override via PERF_BIN env var)
PERF_BIN = os.environ.get("PERF_BIN", "perf")

# If your perf -I output is cumulative on your system (rare), set this to True.
# Default (False) assumes perf -I prints per-interval counts (typical).
PERF_I_IS_CUMULATIVE = False

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
SLOWDOWN_LIMIT = 0.04  # delta in paper (5%)

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
    Uses: perf stat -I <ms> -e cycles:u,instructions:u -p <pid> -x ,
    Reads stderr in nonblocking mode.

    By default assumes perf -I prints PER-INTERVAL counts (typical).
    If your system prints cumulative counts, set PERF_I_IS_CUMULATIVE=True.
    """
    def __init__(self, pid, interval_ms=500):
        self.pid = int(pid)
        self.interval_ms = int(interval_ms)
        self.process = None
        self.valid = False

        # last values seen for the most recent interval line
        self._last_instr_raw = None
        self._last_cycles_raw = None
        self._have_cycles = False
        self._have_instr = False

        # used only when PERF_I_IS_CUMULATIVE=True
        self._prev_instr_raw = None
        self._prev_cycles_raw = None

        self._start()

    def _start(self):
        cmd = [
            PERF_BIN, "stat",
            "-I", str(self.interval_ms),
            "-e", "cycles:u,instructions:u",
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
        if not line:
            return
        s = line.strip()
        if not s or s.startswith("#"):
            return

        parts = s.split(",")
        if len(parts) < 4:
            return

        count_s = parts[1].strip()
        event = parts[3].strip()

        try:
            val = float(count_s)
        except Exception:
            return

        if event == "cycles:u":
            self._last_cycles_raw = val
            self._have_cycles = True
        elif event == "instructions:u":
            self._last_instr_raw = val
            self._have_instr = True

    def _drain(self):
        if not self.valid or self.process is None or self.process.stderr is None:
            return
        try:
            while True:
                line = self.process.stderr.readline()
                if not line:
                    break
                self._parse_perf_line(line)
        except Exception:
            pass
        try:
            if self.process.poll() is not None:
                self.valid = False
        except Exception:
            self.valid = False

    def get_metrics(self):
        """
        Returns: (ipc, di)
          ipc = instructions/cycles over the most recent interval
          di  = instructions in that interval (used for MIPS)
        """
        if not self.valid:
            return 0.0, 0

        self._drain()
        if not (self._have_cycles and self._have_instr):
            return 0.0, 0

        instr = self._last_instr_raw
        cycles = self._last_cycles_raw
        if instr is None or cycles is None or instr <= 0 or cycles <= 0:
            return 0.0, 0

        if PERF_I_IS_CUMULATIVE:
            # convert to deltas explicitly
            if self._prev_instr_raw is None or self._prev_cycles_raw is None:
                self._prev_instr_raw = instr
                self._prev_cycles_raw = cycles
                return 0.0, 0
            di = instr - self._prev_instr_raw
            dc = cycles - self._prev_cycles_raw
            self._prev_instr_raw = instr
            self._prev_cycles_raw = cycles
            if di <= 0 or dc <= 0:
                return 0.0, 0
            ipc = di / dc
            return float(ipc), int(di)

        # typical: perf -I already emits per-interval counts
        ipc = instr / cycles if cycles > 0 else 0.0
        return float(ipc), int(instr)

    def close(self):
        self._stop()


# ================= BETA OPTIMIZER =================
class BetaOptimizer:
    """
    Keeps:
      - mips_table for clean discrete freq points (for fmax + optional others)
      - rolling samples (f_eff, mips) for beta regression

    You update samples every interval.
    You recompute beta only when entering compute-ish phase.
    """
    def __init__(self, window=40):
        self.mips_table = {f: 0.0 for f in FREQ_AVAIL}  # clean discrete points
        self.beta = 1.0
        self.samples = deque(maxlen=window)  # (f_eff, mips)

    def _effective_freq(self, f_low, f_high, t_low, interval_len):
        if f_low == f_high:
            return float(f_low)

        if interval_len <= 0:
            return float(f_high)

        r = max(0.0, min(1.0, t_low / interval_len))
        inv = r * (1.0 / float(f_low)) + (1.0 - r) * (1.0 / float(f_high))
        if inv <= 0:
            return float(f_high)
        return 1.0 / inv

    def update_interval_sample(self, f_low, f_high, t_low, interval_len, mips):
        if mips <= 0:
            return

        f_eff = self._effective_freq(f_low, f_high, t_low, interval_len)
        self.samples.append((float(f_eff), float(mips)))

        # refresh clean table when the interval was at a single discrete freq
        if f_low == f_high and f_low in self.mips_table:
            self.mips_table[f_low] = float(mips)

    def ready(self):
        if self.mips_table[FREQ_MAX] <= 0:
            return False
        # at least some samples below fmax
        return any((m > 0 and f_eff < FREQ_MAX * 0.999) for (f_eff, m) in self.samples)

    def maybe_recompute_beta(self):
        """
        Least squares style:
          beta = sum x_i*(mips_max/mips_i - 1) / sum x_i^2
          x_i = (fmax/f_i - 1)

        Uses rolling samples with f_eff (handles PWM).
        """
        mips_max = self.mips_table[FREQ_MAX]
        if mips_max <= 0:
            return self.beta

        num = 0.0
        den = 0.0

        for f_eff, mips_i in self.samples:
            if mips_i <= 0:
                continue
            if f_eff >= FREQ_MAX * 0.999:
                continue
            x = (FREQ_MAX / float(f_eff)) - 1.0
            if x <= 1e-9:
                continue
            y = (mips_max / float(mips_i)) - 1.0
            num += x * y
            den += x * x

        if den > 1e-12:
            b = num / den
            self.beta = max(0.01, min(2.0, b))

        return self.beta

    def get_f_star(self):
        delta = SLOWDOWN_LIMIT
        b = max(self.beta, 1e-6)
        f_star = FREQ_MAX / (1.0 + delta / b)
        return max(FREQ_MIN, min(FREQ_MAX, f_star))

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
                with open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor", "w") as f:
                    f.write("userspace")
                self.handles[c] = open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed", "w")
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
        cmd = ["pgrep", "-u", os.environ.get("USER", ""), "-f", app_name]
        out = subprocess.check_output(cmd, text=True).strip()
        if not out:
            return {}
        pids = [int(x) for x in out.split()]
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", "r") as f:
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
    pwm_map: { core_id: (freq_low, freq_high, time_at_low_requested) }
    Returns: actual_low_seconds per core (dict)
    """
    start = time.time()
    low_start = {c: start for c in pwm_map.keys()}
    spent_low = {c: 0.0 for c in pwm_map.keys()}

    while True:
        now = time.time()
        elapsed = now - start
        if elapsed >= duration:
            break

        for c, (f_low, f_high, t_low_req) in pwm_map.items():
            if elapsed < t_low_req:
                ctl.set(c, f_low)
            else:
                if low_start[c] is not None:
                    spent_low[c] += max(0.0, now - low_start[c])
                    low_start[c] = None
                ctl.set(c, f_high)

        time.sleep(PWM_RESOLUTION)

    end = time.time()
    for c in pwm_map.keys():
        if low_start[c] is not None:
            spent_low[c] += max(0.0, end - low_start[c])
            low_start[c] = None

    return spent_low


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=str, required=True)
    parser.add_argument("--heartbeat", action="store_true")
    args = parser.parse_args()

    cores = []
    for part in args.cores.split(","):
        if "-" in part:
            s, e = map(int, part.split("-"))
            cores.extend(range(s, e + 1))
        else:
            cores.append(int(part))
    cores = sorted(set(cores))

    print(f"[*] Beta-Integrated PWM Emulator Started. Cores: {cores}")
    print(f"[*] Using perf binary: {PERF_BIN}")
    print(f"[*] PERF_I_IS_CUMULATIVE={PERF_I_IS_CUMULATIVE}")

    ctl = Controller(cores)
    io_det = IODetector()
    util = CoreUtilMonitor(cores)

    ipc_mons = {}  # core -> ProcessIPC
    betas = {c: BetaOptimizer() for c in cores}

    # prev_plan labels what ACTUALLY happened in the previous interval:
    # core -> (f_low, f_high, r_act, t_low_act)
    prev_plan = {c: (FREQ_MAX, FREQ_MAX, 1.0, LOOP_SLEEP) for c in cores}

    # calibration state per core:
    # 0 = need MAX sample, 1 = need MID sample, 2 = ready for beta PWM
    cal_state = {c: 0 for c in cores}

    util.sample()

    try:
        while not stop_event.is_set():
            # 1) refresh pid map + perf monitors
            pmap = scan_pids(APP_NAME, cores)

            for c in list(ipc_mons.keys()):
                if c not in pmap:
                    try:
                        ipc_mons[c].close()
                    except Exception:
                        pass
                    del ipc_mons[c]
                else:
                    if ipc_mons[c].pid != pmap[c]:
                        try:
                            ipc_mons[c].close()
                        except Exception:
                            pass
                        ipc_mons[c] = ProcessIPC(pmap[c], interval_ms=int(LOOP_SLEEP * 1000))

            for c, pid in pmap.items():
                if c not in ipc_mons:
                    ipc_mons[c] = ProcessIPC(pid, interval_ms=int(LOOP_SLEEP * 1000))

            # 2) global metrics
            curr_pids = list(pmap.values())
            io_mb = io_det.get_write_mb(curr_pids, LOOP_SLEEP)
            u_data = util.sample()
            io_override = (io_mb > GLOBAL_IO_THRESHOLD_MB)

            # 3) read perf for the interval that just happened, update beta samples using prev_plan
            per_core = {}
            for c in cores:
                u = u_data.get(c, 0.0)
                ipc = 0.0
                instr_delta = 0

                if c in ipc_mons and ipc_mons[c].valid:
                    ipc, instr_delta = ipc_mons[c].get_metrics()

                # MIPS over the fixed interval length I
                interval_len = LOOP_SLEEP
                mips = ((instr_delta / 1e6) / interval_len) if instr_delta > 0 else 0.0

                f_prev_low, f_prev_high, r_prev, t_prev_low = prev_plan[c]
                if mips > 0:
                    betas[c].update_interval_sample(
                        f_prev_low, f_prev_high, t_prev_low, interval_len, mips
                    )

                # calibration progresses only on clean discrete points (table updated inside beta)
                if cal_state[c] == 0 and betas[c].mips_table[FREQ_MAX] > 0:
                    cal_state[c] = 1
                if cal_state[c] == 1 and betas[c].mips_table[FREQ_MID] > 0:
                    cal_state[c] = 2

                per_core[c] = {"u": u, "ipc": ipc, "di": instr_delta, "mips": mips}

            # 4) decide next interval plan
            pwm_execution_map = {}
            hb = []

            for c in cores:
                u = per_core[c]["u"]
                ipc = per_core[c]["ipc"]
                instr_delta = per_core[c]["di"]
                mips = per_core[c]["mips"]

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
                    if cal_state[c] == 0:
                        state = "CAL_MAX"
                        f_low = f_high = FREQ_MAX
                        r = 1.0
                    elif cal_state[c] == 1:
                        state = "CAL_MID"
                        f_low = f_high = FREQ_MID
                        r = 1.0
                    else:
                        # only recompute beta when we actually need it (compute-ish phase)
                        if betas[c].ready() and len(betas[c].samples) >= 5:
                            betas[c].maybe_recompute_beta()

                        f_star = betas[c].get_f_star()
                        f_star_disp = f_star
                        f_j, f_next, r_calc = betas[c].get_pwm_params(f_star)
                        f_low, f_high, r = f_j, f_next, r_calc
                        state = f"B{betas[c].beta:.2f}"

                t_low_req = r * LOOP_SLEEP
                pwm_execution_map[c] = (f_low, f_high, t_low_req)

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
                print(f"|IO:{io_mb:.1f}MB| " + " ".join(hb))
                sys.stdout.flush()

            # 5) actuate interval and measure actual low time, then label prev_plan with what really happened
            actual_low = emulate_pwm(ctl, pwm_execution_map, LOOP_SLEEP)
            for c in cores:
                f_low, f_high, t_low_req = pwm_execution_map[c]
                t_low_act = actual_low.get(c, t_low_req)
                r_act = max(0.0, min(1.0, t_low_act / LOOP_SLEEP))
                prev_plan[c] = (f_low, f_high, r_act, t_low_act)

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
