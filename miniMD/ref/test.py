#!/usr/bin/env python3
import os, sys, time, argparse, subprocess, signal, threading, fcntl, math, random

APP_NAME = "miniMD"
PERF_BIN = os.environ.get("PERF_BIN", "perf")

PERF_I_IS_CUMULATIVE = False
PERF_FAIL_STREAK_LIMIT = 5
PERF_STARTUP_GRACE = 3

GEAR_EVENTS = [
    "cycles:u",
    "instructions:u",
    "cache-references:u",
    "cache-misses:u",
]

DEFAULT_INTERVAL_S = 0.5

LAMBDA_SLOW_DEFAULT = 2.5
POWER_SCALE_DEFAULT = 1.0

EPS_START = 0.20
EPS_MIN   = 0.05
EPS_DECAY = 0.999
LR        = 0.03
L2        = 1e-4
CLIP_W    = 10.0

GLOBAL_IO_THRESHOLD_MB = 15.0
UTIL_IDLE_THRESHOLD = 10.0
COMPUTE_UTIL_CAP = 70.0

RAPL_FILE = "/sys/class/powercap/intel-rapl:0/energy_uj"
MAX_RAPL_FILE = "/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

stop_event = threading.Event()
def _sig_handler(signum, frame):
    stop_event.set()
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

# ----------------------------
# DVFS / freq reads
# ----------------------------
def read_cur_freq_khz(core):
    p = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_cur_freq"
    try:
        with open(p, "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def write_file(path, s):
    try:
        with open(path, "w") as f:
            f.write(str(s))
        return True
    except Exception:
        return False

def set_userspace_and_open_setspeed(core):
    govp = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
    setp = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_setspeed"
    ok_gov = write_file(govp, "userspace")
    try:
        f = open(setp, "w")
        return ok_gov, f
    except Exception:
        return ok_gov, None

def set_freq(handle, freq_khz):
    try:
        handle.seek(0)
        handle.write(str(int(freq_khz)))
        handle.flush()
        return True
    except Exception:
        return False

class DVFSHandles:
    def __init__(self, reserved_core=None):
        self.reserved = reserved_core
        self.handles = {}   # cpu -> file handle
        self.gov_ok = set()

    def ensure(self, cpu):
        if self.reserved is not None and cpu == self.reserved:
            return False
        if cpu in self.handles:
            return True
        ok_gov, h = set_userspace_and_open_setspeed(cpu)
        if ok_gov:
            self.gov_ok.add(cpu)
        if h is not None:
            self.handles[cpu] = h
            return True
        return False

    def set(self, cpu, freq_khz):
        if self.reserved is not None and cpu == self.reserved:
            return False
        if cpu not in self.handles:
            if not self.ensure(cpu):
                return False
        return set_freq(self.handles[cpu], freq_khz)

    def reset_all(self, freq_khz):
        for cpu, h in list(self.handles.items()):
            if self.reserved is not None and cpu == self.reserved:
                continue
            try:
                set_freq(h, freq_khz)
            except Exception:
                pass

    def close(self):
        for h in self.handles.values():
            try: h.close()
            except Exception: pass
        self.handles.clear()

# ----------------------------
# RAPL helpers
# ----------------------------
def _read_int(path, default=0):
    try:
        with open(path, "r") as f:
            return int(f.read().strip())
    except Exception:
        return default

def read_energy_uj():
    return _read_int(RAPL_FILE, 0)

def read_max_uj():
    return _read_int(MAX_RAPL_FILE, 262143328850)

def energy_diff_uj(before, after, maxv):
    d = after - before
    if d < 0:
        d += maxv
    return d

class OverheadMeter:
    def __init__(self):
        self.max_uj = read_max_uj()
        self.e0 = read_energy_uj()
        self.t0 = time.time()
        self.ticks = 0
    def tick(self):
        self.ticks += 1
    def finish(self):
        e1 = read_energy_uj()
        t1 = time.time()
        duj = energy_diff_uj(self.e0, e1, self.max_uj)
        wall = t1 - self.t0
        return {
            "wall_s": wall,
            "energy_j": duj / 1e6,
            "ticks": self.ticks,
            "hz": (self.ticks / wall) if wall > 1e-9 else 0.0
        }

# ----------------------------
# Marker / phase helpers
# ----------------------------
def read_marker(marker_path):
    try:
        with open(marker_path, "r") as f:
            return f.read().strip()
    except Exception:
        return ""

def marker_onehot(s):
    if not s:
        return (0.0, 0.0, 0.0)
    if s.startswith("IO_"):
        return (1.0, 0.0, 0.0)
    if s.startswith("COMM_"):
        return (0.0, 1.0, 0.0)
    if s.startswith("COMP_") or s.startswith("COMPUTE_"):
        return (0.0, 0.0, 1.0)
    return (0.0, 0.0, 0.0)

def is_computeish(marker_s):
    if not marker_s:
        return True
    return not marker_s.startswith("IO_")

# ----------------------------
# Util monitor
# ----------------------------
class CoreUtilMonitor:
    def __init__(self, cores=None):
        self.cores = set(cores or [])
        self.prev = {}
        try:
            self.f = open("/proc/stat", "r")
        except Exception:
            self.f = None

    def update_cores(self, cores):
        self.cores = set(cores)

    def sample(self):
        if self.f is None or not self.cores:
            return {}
        self.f.seek(0)
        res = {}
        for line in self.f:
            if not line.startswith("cpu"):
                continue
            parts = line.split()
            if parts[0] == "cpu":
                continue
            try:
                c = int(parts[0][3:])
            except Exception:
                continue
            if c not in self.cores:
                continue
            vals = [int(x) for x in parts[1:]]
            idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
            total = sum(vals)
            if c in self.prev:
                dt = total - self.prev[c][0]
                di = idle - self.prev[c][1]
                res[c] = (1.0 - (di / dt)) * 100.0 if dt > 0 else 0.0
            self.prev[c] = (total, idle)
        return res

# ----------------------------
# IO detector
# ----------------------------
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
                try: self.handles[pid].close()
                except Exception: pass
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

# ----------------------------
# perf stat per PID
# ----------------------------
class PerfStatPID:
    def __init__(self, pid, interval_ms):
        self.pid = int(pid)
        self.interval_ms = int(interval_ms)
        self.process = None
        self.valid = False
        self.last_raw = {}
        self.have = set()
        self.prev_raw = {}
        self._start()

    def _start(self):
        cmd = [
            PERF_BIN, "stat",
            "-I", str(self.interval_ms),
            "-e", ",".join(GEAR_EVENTS),
            "-p", str(self.pid),
            "-x", ",",
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

    def close(self):
        if self.process is None:
            return
        try: self.process.terminate()
        except Exception: pass
        try: self.process.kill()
        except Exception: pass
        try:
            if self.process.stderr:
                self.process.stderr.close()
        except Exception:
            pass
        self.process = None
        self.valid = False

    def _parse_line(self, line):
        s = (line or "").strip()
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
        if event in GEAR_EVENTS:
            self.last_raw[event] = val
            self.have.add(event)

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

    def get_interval_counts(self):
        if not self.valid:
            return {}
        self._drain()
        if not self.have:
            return {}
        if not PERF_I_IS_CUMULATIVE:
            return dict(self.last_raw)

        out = {}
        for ev, v in self.last_raw.items():
            pv = self.prev_raw.get(ev)
            if pv is None:
                self.prev_raw[ev] = v
                continue
            d = v - pv
            self.prev_raw[ev] = v
            if d > 0:
                out[ev] = d
        return out

# ----------------------------
# Scratch file reader (NEW)
# ----------------------------
def scratch_path_from_marker(marker_path: str) -> str:
    return marker_path + ".ranks"

def read_rank_scratch(path: str):
    """
    Expected format:
      ts=... phase=... nprocs=...
      rank=0 pid=123 cpu=4 freq_khz=2000000 omp_threads=...
      ...
    Returns (cpu_to_pid, rank0_cpu) or ({}, None) if missing/bad.
    """
    try:
        with open(path, "r") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except Exception:
        return {}, None

    cpu_to_pid = {}
    rank0_cpu = None

    for ln in lines[1:]:
        toks = ln.split()
        kv = {}
        for t in toks:
            if "=" in t:
                k, v = t.split("=", 1)
                kv[k.strip()] = v.strip()
        try:
            r = int(kv.get("rank", "-1"))
            pid = int(kv.get("pid", "0"))
            cpu = int(kv.get("cpu", "-1"))
        except Exception:
            continue
        if cpu < 0 or pid <= 0:
            continue
        cpu_to_pid[cpu] = pid
        if r == 0:
            rank0_cpu = cpu

    return cpu_to_pid, rank0_cpu

# ----------------------------
# Bandit
# ----------------------------
class BanditPolicy:
    def __init__(self, n_actions, n_features):
        self.n_actions = n_actions
        self.n_features = n_features
        self.W = [[0.0] * n_features for _ in range(n_actions)]
        self.eps = EPS_START

    @staticmethod
    def _dot(w, x):
        return sum(wi * xi for wi, xi in zip(w, x))

    def select(self, x):
        if random.random() < self.eps:
            return random.randrange(self.n_actions)
        qs = [self._dot(self.W[a], x) for a in range(self.n_actions)]
        return max(range(self.n_actions), key=lambda a: qs[a])

    def update(self, x, a, r):
        pred = self._dot(self.W[a], x)
        err = (r - pred)
        for i in range(self.n_features):
            self.W[a][i] += LR * (err * x[i] - L2 * self.W[a][i])
            if self.W[a][i] > CLIP_W: self.W[a][i] = CLIP_W
            if self.W[a][i] < -CLIP_W: self.W[a][i] = -CLIP_W

    def decay_eps(self):
        self.eps = max(EPS_MIN, self.eps * EPS_DECAY)

# ----------------------------
# Features
# ----------------------------
def clamp(x, lo, hi):
    return lo if x < lo else hi if x > hi else x

def safe_div(a, b):
    return a / b if b else 0.0

def meta_features(u_pct, counts, io_mb_s, cur_freq_khz, fmin, fmax, marker_s, perf_enabled):
    cyc = counts.get("cycles:u", 0.0) if perf_enabled else 0.0
    ins = counts.get("instructions:u", 0.0) if perf_enabled else 0.0
    cmiss = counts.get("cache-misses:u", 0.0) if perf_enabled else 0.0

    ipc = safe_div(ins, cyc)
    mpki = safe_div(cmiss * 1000.0, ins)

    u = clamp(u_pct / 100.0, 0.0, 1.0)
    io = clamp(io_mb_s / 50.0, 0.0, 1.0)

    f = 0.0
    if fmax > fmin:
        f = clamp((cur_freq_khz - fmin) / float(fmax - fmin), 0.0, 1.0)

    ipc_n = clamp(ipc / 2.0, 0.0, 1.0)
    mpki_n = clamp(math.log1p(max(0.0, mpki)) / 5.0, 0.0, 1.0)

    is_io, is_comm, is_comp = marker_onehot(marker_s)

    feat = [1.0, u, ipc_n, mpki_n, io, f, is_io, is_comm, is_comp]
    return feat, ipc, ins, cyc

def parse_cores_csv(s):
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a, b = int(a), int(b)
            if b < a: a, b = b, a
            out.extend(range(a, b + 1))
        else:
            out.append(int(part))
    return sorted(set(out))

# ----------------------------
# MAIN
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker-cores", required=True)     # kept for eval.sh
    ap.add_argument("--rank0-core", required=True, type=int)  # kept for eval.sh
    ap.add_argument("--marker", required=True)
    ap.add_argument("--freq-high", required=True, type=int)
    ap.add_argument("--freq-mid", required=True, type=int)
    ap.add_argument("--freq-low", required=True, type=int)
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_S)
    ap.add_argument("--heartbeat", action="store_true")
    ap.add_argument("--no-actuate", action="store_true")
    ap.add_argument("--lambda-slow", type=float, default=LAMBDA_SLOW_DEFAULT)
    ap.add_argument("--power-scale", type=float, default=POWER_SCALE_DEFAULT)
    args = ap.parse_args()

    requested = parse_cores_csv(args.worker_cores)
    n_workers = len(requested)

    marker_path = args.marker
    scratch_path = scratch_path_from_marker(marker_path)

    interval_s = float(args.interval)
    FREQS = sorted([int(args.freq_low), int(args.freq_mid), int(args.freq_high)])
    fmin, fmid, fmax = FREQS[0], FREQS[1], FREQS[-1]

    aff = os.sched_getaffinity(0)
    reserved = sorted(list(aff))[0] if aff else None

    print("Controller start (prefers miniMD scratchpad):", flush=True)
    print(f"  requested_worker_cores(arg)={requested} (count={n_workers})", flush=True)
    print(f"  marker={marker_path}", flush=True)
    print(f"  scratch={scratch_path}", flush=True)
    print(f"  freqs: HIGH={args.freq_high} MID={args.freq_mid} LOW={args.freq_low}", flush=True)
    print(f"  reserved(monitor)={reserved}", flush=True)
    print(f"  interval={interval_s}s perf={PERF_BIN}", flush=True)
    print(f"  no_actuate={args.no_actuate}", flush=True)
    print("", flush=True)

    meter = OverheadMeter()
    dvfs = DVFSHandles(reserved_core=reserved)
    util_mon = CoreUtilMonitor([])
    io_det = IODetector()

    perf_mons_by_pid = {}
    perf_enabled = True
    perf_fail_streak = 0
    perf_intervals_seen = 0

    n_features = 9
    policies = {}
    baseline_mips = {}

    warmup_intervals = 6
    interval_count = 0

    last_E = read_energy_uj()
    last_t = time.time()
    max_uj = read_max_uj()

    last_action = {}
    last_feat = {}

    rank0_cpu = None

    try:
        while not stop_event.is_set():
            t0 = time.time()
            meter.tick()

            # 1) Prefer scratchpad mapping from miniMD
            cpu_to_pid, r0 = read_rank_scratch(scratch_path)
            if cpu_to_pid:
                active_cpu_to_pid = cpu_to_pid
                if r0 is not None:
                    rank0_cpu = r0
            else:
                # If scratch isn't available yet, do nothing risky:
                # wait for mapping rather than guessing incorrectly.
                active_cpu_to_pid = {}

            active_cpus = sorted(active_cpu_to_pid.keys())
            active_pids = list(active_cpu_to_pid.values())

            util_mon.update_cores(active_cpus)

            if not args.no_actuate:
                for cpu in active_cpus:
                    dvfs.ensure(cpu)

            # 2) perf monitors for active PIDs
            interval_ms = max(1, int(interval_s * 1000))
            if perf_enabled:
                for pid in list(perf_mons_by_pid.keys()):
                    if pid not in active_pids:
                        try: perf_mons_by_pid[pid].close()
                        except Exception: pass
                        del perf_mons_by_pid[pid]
                for pid in active_pids:
                    if pid not in perf_mons_by_pid:
                        perf_mons_by_pid[pid] = PerfStatPID(pid, interval_ms)

            # 3) sample
            u_data = util_mon.sample()
            io_mb_s = io_det.get_write_mb(active_pids, interval_s)
            marker_s = read_marker(marker_path)

            # 4) power
            power_w = None
            now_E = read_energy_uj()
            now_t = time.time()
            if last_E is not None and now_E is not None:
                duj = energy_diff_uj(last_E, now_E, max_uj)
                dt = max(1e-6, now_t - last_t)
                power_w = (duj / 1e6) / dt
            last_E, last_t = now_E, now_t

            # If we don't have active ranks yet, just heartbeat and keep looping.
            if not active_cpus:
                if args.heartbeat:
                    pw = power_w if power_w is not None else -1.0
                    print(f"|mode:WAIT|active:[]|IO:{io_mb_s:.1f}MB/s|P:{pw:.1f}W|M='{marker_s}'|", flush=True)
                time.sleep(max(0.01, interval_s))
                continue

            # 5) features per active cpu
            per_core = {}
            any_counts = False
            for cpu in active_cpus:
                pid = active_cpu_to_pid.get(cpu)
                u = u_data.get(cpu, 0.0)
                cur_f = read_cur_freq_khz(cpu)

                counts = {}
                if perf_enabled and pid in perf_mons_by_pid and perf_mons_by_pid[pid].valid:
                    counts = perf_mons_by_pid[pid].get_interval_counts()
                if counts:
                    any_counts = True

                feat, ipc_raw, ins, cyc = meta_features(
                    u, counts, io_mb_s, cur_f, fmin, fmax, marker_s, perf_enabled
                )
                mips = ((ins / 1e6) / interval_s) if ins > 0 else 0.0

                per_core[cpu] = {"u": u, "feat": feat, "ipc": ipc_raw, "ins": ins, "mips": mips, "f": cur_f}
                if cpu not in policies:
                    policies[cpu] = BanditPolicy(len(FREQS), n_features)

            # 6) perf health
            if perf_enabled:
                perf_intervals_seen += 1
                if perf_intervals_seen > PERF_STARTUP_GRACE:
                    if not any_counts:
                        perf_fail_streak += 1
                    else:
                        perf_fail_streak = 0
                    if perf_fail_streak >= PERF_FAIL_STREAK_LIMIT:
                        perf_enabled = False
                        for pm in list(perf_mons_by_pid.values()):
                            try: pm.close()
                            except Exception: pass
                        perf_mons_by_pid.clear()
                        print("[WARN] perf appears blocked/no counts; switching to NO-PERF fallback mode.", flush=True)

            # 7) warmup baseline at max
            interval_count += 1
            if interval_count <= warmup_intervals:
                if not args.no_actuate:
                    for cpu in active_cpus:
                        dvfs.set(cpu, fmax)

                if perf_enabled:
                    for cpu in active_cpus:
                        m = per_core[cpu]["mips"]
                        if m > 0:
                            baseline_mips[cpu] = m if cpu not in baseline_mips else (0.8 * baseline_mips[cpu] + 0.2 * m)

                if args.heartbeat:
                    pw = power_w if power_w is not None else -1.0
                    mode = "PERF" if perf_enabled else "NOPERF"
                    print(f"[WARMUP {interval_count}/{warmup_intervals}] mode={mode} active={active_cpus} IO:{io_mb_s:.1f}MB/s P:{pw:.1f}W M='{marker_s}'", flush=True)

                sleep_left = interval_s - (time.time() - t0)
                if sleep_left > 0:
                    time.sleep(sleep_left)
                continue

            # 8) bandit update
            for cpu in active_cpus:
                a_prev = last_action.get(cpu)
                x_prev = last_feat.get(cpu)
                if a_prev is None or x_prev is None:
                    continue

                slow = 0.0
                b = baseline_mips.get(cpu) or 0.0
                m = per_core[cpu]["mips"]
                if b > 1e-6 and m > 0:
                    slow = max(0.0, (b - m) / b)

                if power_w is None:
                    r = -args.lambda_slow * slow
                else:
                    r = -(args.power_scale * power_w) - (args.lambda_slow * slow)

                policies[cpu].update(x_prev, a_prev, r)

            # 9) choose actions
            io_override = (io_mb_s > GLOBAL_IO_THRESHOLD_MB) or marker_s.startswith("IO_")
            hb_parts = []
            mode = "PERF" if perf_enabled else "NOPERF"

            for cpu in active_cpus:
                u = per_core[cpu]["u"]
                x = per_core[cpu]["feat"]
                ipc = per_core[cpu]["ipc"]
                mips = per_core[cpu]["mips"]

                if io_override or u < UTIL_IDLE_THRESHOLD or u <= 1.0:
                    a = 0
                    state = "IO/IDLE"
                else:
                    a = policies[cpu].select(x)
                    state = f"eps{policies[cpu].eps:.2f}"

                target = FREQS[a]

                # COMM boost for rank0 (from scratch)
                if marker_s.startswith("COMM_") and rank0_cpu is not None and cpu == rank0_cpu:
                    target = fmax
                    state = "COMM_R0"

                # compute cap
                if is_computeish(marker_s) and u >= COMPUTE_UTIL_CAP and target < fmid:
                    target = fmid
                    if state != "COMM_R0":
                        state = "CAP_MID"

                if not args.no_actuate:
                    dvfs.set(cpu, target)

                last_action[cpu] = a
                last_feat[cpu] = x
                policies[cpu].decay_eps()

                if args.heartbeat:
                    hb_parts.append(f"C{cpu}[{state}|U:{u:4.0f}%|IPC:{ipc:4.2f}|M:{mips:6.1f}|F:{int(target/1000)}]")

            if args.heartbeat:
                pw = power_w if power_w is not None else -1.0
                print(f"|mode:{mode}|active:{active_cpus}|IO:{io_mb_s:.1f}MB/s|P:{pw:.1f}W|M='{marker_s}'| " + " ".join(hb_parts), flush=True)

            sleep_left = interval_s - (time.time() - t0)
            if sleep_left > 0:
                time.sleep(sleep_left)

    finally:
        if not args.no_actuate:
            dvfs.reset_all(fmax)
        dvfs.close()

        for pm in list(perf_mons_by_pid.values()):
            try: pm.close()
            except Exception: pass

        stats = meter.finish()
        print(
            f"CTRL_SUMMARY wall_s={stats['wall_s']:.6f} "
            f"energy_j={stats['energy_j']:.6f} "
            f"samples={stats['ticks']} hz={stats['hz']:.2f}",
            flush=True
        )

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
