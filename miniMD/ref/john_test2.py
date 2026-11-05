#!/usr/bin/env python3
"""
Phase-Aware IO/COMM Power Controller for miniMD (user-space)
===========================================================
Watches phase_marker.txt (written by rank 0) and applies user-space optimizations
to reduce energy during IO + communication phases.

Implements the optimizations we discussed:

1) Reduce wakeups from the controller itself
   - Adaptive polling (fast in compute, slow in IO/COMM)
   - Optional inotify (if available) to become event-driven

2) IO/COMM scheduling knobs from user space (no sudo):
   - renice: increase nice during IO/COMM (lower CPU priority)
   - ionice: lower IO priority during IO (best-effort low), optional

3) COMM critical-path boosting:
   - Detect the "most active" rank during COMM by reading /proc/<pid>/stat (utime+stime deltas)
   - Boost that rank's core to HIGH (or MID) and park others LOW

4) Thrash prevention:
   - Minimum dwell time between frequency changes
   - Phase-duration gating: only apply IO/COMM policies if phase lasts long enough

Notes:
- You SHOULD still pin this monitor to a spare core via taskset.
- This script can optionally discover rank PIDs for your miniMD job by scanning /proc for the binary name,
  and filtering by which CPU each PID is running on (worker core set).
- If pid discovery fails, the script still does DVFS changes based on phase marker.

Example:
  taskset -c 27 python3 comm_freq_controller.py \
    --worker-cores "4-11" \
    --binary ./miniMD_openmpi \
    --rank0-core 4 \
    --log monitor.csv

If you already have rank PIDs:
  taskset -c 27 python3 comm_freq_controller.py \
    --worker-cores "4-11" \
    --pids "1234,1235,..." \
    --rank0-core 4
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime
from typing import List, Optional, Tuple

# ============ DEFAULTS ============
PHASE_MARKER = "phase_marker.txt"

# Default frequencies (kHz)
DEFAULT_FREQ_HIGH = 2400000   # 2.4 GHz (frnt115 max)
DEFAULT_FREQ_MID  = 1600000   # 1.6 GHz
DEFAULT_FREQ_LOW  = 1200000   # 1.2 GHz

RESERVED_CORE = 31  # never touch

# Adaptive polling (seconds)
POLL_FAST = 0.05    # compute: 50ms
POLL_SLOW = 0.30    # IO/COMM: 300ms (reduces wakeups)

# Thrash prevention
DEFAULT_MIN_DWELL_S = 1.5      # minimum time between freq changes
DEFAULT_MIN_PHASE_S = 0.25     # only apply IO/COMM policy if the phase likely lasts >= this

# Nice/ionice policies
DEFAULT_NICE_IOCOMM = 15       # increase nice during IO/COMM (lower priority)
DEFAULT_IONICE_CLASS_IO = 2    # best-effort
DEFAULT_IONICE_LEVEL_IO = 7    # low priority within BE class
DEFAULT_IONICE_CLASS_COMPUTE = 2
DEFAULT_IONICE_LEVEL_COMPUTE = 0


# ============ CORE / CPUSET HELPERS ============

def parse_core_list(s: str) -> List[int]:
    """Parse '0-3,6,8-9' -> sorted unique list of ints."""
    cores = set()
    if not s:
        return []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a = int(a.strip()); b = int(b.strip())
            if b < a:
                a, b = b, a
            for c in range(a, b + 1):
                cores.add(c)
        else:
            cores.add(int(part))
    return sorted(cores)

def get_cpuset_str_for_pid(pid: int) -> Optional[str]:
    """Returns cpuset string like '0-3,6,8-9' for a pid using taskset -cp."""
    try:
        out = subprocess.check_output(["taskset", "-cp", str(pid)], text=True).strip()
        if ":" in out:
            return out.split(":", 1)[1].strip()
    except Exception:
        return None
    return None

def pick_monitor_core(worker_cores: List[int]) -> Optional[int]:
    """Pick highest available CPU in this process affinity not in worker_cores."""
    cpuset = get_cpuset_str_for_pid(os.getpid())
    if not cpuset:
        return None
    avail = parse_core_list(cpuset)
    wset = set(worker_cores)
    candidates = [c for c in avail if c not in wset]
    return max(candidates) if candidates else None


# ============ CPUFREQ HELPERS ============

def _write_sysfs(path: str, val: str, dry_run: bool) -> bool:
    if dry_run:
        return True
    try:
        with open(path, "w") as f:
            f.write(val)
        return True
    except Exception:
        return False

def set_freq(core: int, freq_khz: int, dry_run: bool = False) -> bool:
    """Set CPU frequency for a specific core (skips reserved core)."""
    if core == RESERVED_CORE:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_setspeed"
    return _write_sysfs(path, str(int(freq_khz)), dry_run)

def set_governor(core: int, gov: str, dry_run: bool = False) -> bool:
    """Set governor for a specific core (skips reserved core)."""
    if core == RESERVED_CORE:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
    return _write_sysfs(path, gov, dry_run)


# ============ PHASE MARKER HELPERS ============

def read_phase_marker(marker_path: str) -> Optional[str]:
    try:
        if os.path.exists(marker_path):
            with open(marker_path, "r") as f:
                return f.read().strip()
    except Exception:
        pass
    return None

def parse_phase(content: Optional[str]) -> Tuple[str, int]:
    """
    Returns (phase, data_bytes)
    phase in {COMPUTE, IO, COMMUNICATION}
    """
    data_bytes = 0
    if not content:
        return ("COMPUTE", 0)

    if content.startswith("COMM_START"):
        parts = content.split()
        if len(parts) >= 2:
            try:
                data_bytes = int(parts[1])
            except ValueError:
                data_bytes = 0
        return ("COMMUNICATION", data_bytes)
    if content.startswith("COMM_END"):
        return ("COMPUTE", 0)

    if content.startswith("IO_START"):
        return ("IO", 0)
    if content.startswith("IO_END"):
        return ("COMPUTE", 0)

    if content.startswith("COMPUTE_RESUME"):
        return ("COMPUTE", 0)

    return ("COMPUTE", 0)


# ============ PID DISCOVERY / PROC HELPERS ============

def parse_pid_list(s: str) -> List[int]:
    """
    Supports "123,456,700-710"
    """
    pids = set()
    if not s:
        return []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a = int(a.strip()); b = int(b.strip())
            if b < a:
                a, b = b, a
            for x in range(a, b + 1):
                pids.add(x)
        else:
            pids.add(int(part))
    return sorted(pids)

def _read_proc_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read().replace(b"\x00", b" ").decode(errors="ignore").strip()
        return raw
    except Exception:
        return ""

def _read_proc_stat_processor(pid: int) -> Optional[int]:
    """
    /proc/<pid>/stat field 39 (1-indexed), index 38 (0-indexed in split) is processor number.
    """
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            parts = f.read().split()
        if len(parts) > 38:
            return int(parts[38])
    except Exception:
        return None
    return None

def discover_rank_pids(worker_cores: List[int], binary: str, want_n: int, user_uid: Optional[int]) -> List[int]:
    """
    Best-effort PID discovery:
      - same uid
      - cmdline contains basename(binary) or full binary path
      - running on a worker core (processor field)
    """
    bn = os.path.basename(binary)
    wset = set(worker_cores)
    pids = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            st = os.stat(f"/proc/{pid}")
            if user_uid is not None and st.st_uid != user_uid:
                continue
        except Exception:
            continue

        cmd = _read_proc_cmdline(pid)
        if not cmd:
            continue
        if (bn not in cmd) and (binary not in cmd):
            continue

        cpu = _read_proc_stat_processor(pid)
        if cpu is None or cpu not in wset:
            continue

        pids.append(pid)

    pids = sorted(set(pids))[:want_n] if want_n > 0 else sorted(set(pids))
    return pids

def read_cpu_time_jiffies(pid: int) -> Optional[int]:
    """
    Return utime+stime (jiffies) from /proc/<pid>/stat for critical-path detection.
    """
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            parts = f.read().split()
        # fields: utime=14, stime=15 (1-indexed)
        if len(parts) > 14:
            ut = int(parts[13])
            st = int(parts[14])
            return ut + st
    except Exception:
        return None
    return None

def pid_to_core(pid: int) -> Optional[int]:
    """Best-effort: current processor core from /proc/<pid>/stat."""
    return _read_proc_stat_processor(pid)


# ============ NICE / IONICE HELPERS ============

def renice_pid(pid: int, nice_val: int) -> bool:
    """
    Increase nice (lower priority). Decreasing nice usually requires privileges.
    """
    try:
        os.setpriority(os.PRIO_PROCESS, pid, int(nice_val))
        return True
    except Exception:
        return False

def ionice_pid(pid: int, cls: int, level: int) -> bool:
    """
    Best-effort ionice via external command.
      cls: 1=rt, 2=be, 3=idle
    """
    try:
        subprocess.run(
            ["ionice", "-c", str(cls), "-n", str(level), "-p", str(pid)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


# ============ OPTIONAL INOTIFY (best effort) ============

class MarkerWatcher:
    """
    If inotify_simple is available, use it to block until marker changes.
    Otherwise fallback to polling.
    """
    def __init__(self, marker_path: str):
        self.marker_path = marker_path
        self._use_inotify = False
        self._inotify = None
        self._wd = None
        self._dir = os.path.dirname(marker_path) or "."
        self._base = os.path.basename(marker_path)

        try:
            from inotify_simple import INotify, flags  # type: ignore
            self._flags = flags
            ino = INotify()
            wd = ino.add_watch(self._dir, flags.CREATE | flags.MODIFY | flags.MOVED_TO)
            self._use_inotify = True
            self._inotify = ino
            self._wd = wd
        except Exception:
            self._use_inotify = False

    def wait_for_change(self, timeout_s: float) -> None:
        """
        Wait until marker likely changed or timeout.
        """
        if not self._use_inotify:
            time.sleep(timeout_s)
            return

        try:
            events = self._inotify.read(timeout=int(timeout_s * 1000))
            # We don't strictly filter filename; cheap and fine.
            _ = events
        except Exception:
            # fallback silently
            time.sleep(timeout_s)


# ============ MAIN CONTROLLER ============

def main():
    parser = argparse.ArgumentParser(description="Phase-aware IO/COMM power controller for miniMD")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--workers", type=int,
                      help="LEGACY: number of worker cores (assumes cores 0..N-1)")
    mode.add_argument("--worker-cores", type=str,
                      help='Explicit worker core list, e.g. "2,4,6,10-13"')

    parser.add_argument("--rank0-core", type=int, default=None,
                        help="Core ID for MPI rank 0. Default: first worker core.")

    # Frequencies
    parser.add_argument("--freq-high", type=int, default=DEFAULT_FREQ_HIGH)
    parser.add_argument("--freq-mid", type=int, default=DEFAULT_FREQ_MID)
    parser.add_argument("--freq-low", type=int, default=DEFAULT_FREQ_LOW)

    # Marker + logging
    parser.add_argument("--marker", type=str, default=PHASE_MARKER)
    parser.add_argument("--log", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")

    # Thrash controls
    parser.add_argument("--min-dwell-s", type=float, default=DEFAULT_MIN_DWELL_S)
    parser.add_argument("--min-phase-s", type=float, default=DEFAULT_MIN_PHASE_S)

    # Controller wakeup controls
    parser.add_argument("--poll-fast", type=float, default=POLL_FAST)
    parser.add_argument("--poll-slow", type=float, default=POLL_SLOW)
    parser.add_argument("--use-inotify", action="store_true",
                        help="Use inotify if available to reduce wakeups (best effort).")

    # PID-aware scheduling knobs
    parser.add_argument("--pids", type=str, default=None,
                        help='Explicit rank PID list/ranges, e.g. "123,124,200-207"')
    parser.add_argument("--binary", type=str, default=None,
                        help="Binary name/path to help auto-discover rank PIDs (e.g., ./miniMD_openmpi)")
    parser.add_argument("--np", type=int, default=0,
                        help="Expected number of rank PIDs for discovery (optional).")

    parser.add_argument("--nice-iocomm", type=int, default=DEFAULT_NICE_IOCOMM)
    parser.add_argument("--ionice-io-class", type=int, default=DEFAULT_IONICE_CLASS_IO)
    parser.add_argument("--ionice-io-level", type=int, default=DEFAULT_IONICE_LEVEL_IO)
    parser.add_argument("--ionice-compute-class", type=int, default=DEFAULT_IONICE_CLASS_COMPUTE)
    parser.add_argument("--ionice-compute-level", type=int, default=DEFAULT_IONICE_LEVEL_COMPUTE)
    parser.add_argument("--disable-ionice", action="store_true")
    parser.add_argument("--disable-renice", action="store_true")

    # Optional: just prints what monitor core you'd want to taskset to
    parser.add_argument("--suggest-monitor-core", action="store_true",
                        help="Print an auto-picked monitor core (from cpuset) and exit.")

    args = parser.parse_args()

    # Worker cores
    if args.worker_cores:
        worker_cores = parse_core_list(args.worker_cores)
        if not worker_cores:
            print("ERROR: --worker-cores parsed to empty list.")
            sys.exit(1)
    else:
        n = args.workers
        if n <= 0:
            print("ERROR: --workers must be > 0")
            sys.exit(1)
        worker_cores = list(range(n))

    if RESERVED_CORE in worker_cores:
        print(f"ERROR: worker core list includes reserved core {RESERVED_CORE}. Remove it.")
        sys.exit(1)

    # rank0 core
    rank0_core = args.rank0_core if args.rank0_core is not None else worker_cores[0]
    if rank0_core not in worker_cores:
        print(f"ERROR: rank0-core {rank0_core} not in worker core list {worker_cores}")
        sys.exit(1)

    # monitor suggestion
    if args.suggest_monitor_core:
        mon = pick_monitor_core(worker_cores)
        if mon is None:
            print("NO_MONITOR_CORE_FOUND")
            sys.exit(2)
        print(mon)
        sys.exit(0)

    marker_path = args.marker
    dry_run = args.dry_run

    # Logging
    log_file = None
    if args.log:
        log_file = open(args.log, "w")
        log_file.write("timestamp,phase,policy,boost_core,boost_pid,rank0_core,high_khz,mid_khz,low_khz,data_bytes\n")
        log_file.flush()

    # Inotify watcher (best effort)
    watcher = MarkerWatcher(marker_path) if args.use_inotify else None

    # Info banner
    mon_guess = pick_monitor_core(worker_cores)
    print("[MON] Phase-aware IO/COMM power controller")
    print(f"[MON] Worker cores: {worker_cores}")
    print(f"[MON] Rank0 core: {rank0_core}")
    if mon_guess is not None:
        print(f"[MON] (FYI) Suggested monitor core (from your cpuset): {mon_guess}")
    print(f"[MON] Reserved core: {RESERVED_CORE} (never touched)")
    print(f"[MON] Freq HIGH/MID/LOW: {args.freq_high/1000:.0f}/{args.freq_mid/1000:.0f}/{args.freq_low/1000:.0f} MHz")
    print(f"[MON] Marker: {marker_path}")
    print(f"[MON] Dry run: {dry_run}")
    print(f"[MON] Wakeups: compute poll={args.poll_fast*1000:.0f}ms, IO/COMM poll={args.poll_slow*1000:.0f}ms, inotify={bool(args.use_inotify)}")
    print(f"[MON] Thrash controls: min_dwell={args.min_dwell_s:.2f}s, min_phase={args.min_phase_s:.2f}s")
    print(f"[MON] renice enabled: {not args.disable_renice}, ionice enabled: {not args.disable_ionice}")
    print()

    # Set governor to userspace
    print("[MON] Setting worker cores to 'userspace' governor...")
    ok = 0
    for c in worker_cores:
        if set_governor(c, "userspace", dry_run):
            ok += 1
    print(f"[MON] Governor set on {ok}/{len(worker_cores)} worker cores")
    if ok == 0 and not dry_run:
        print("[MON] WARNING: governor writes failed on all cores (permission/governor issue).")

    # Initial compute policy: all HIGH
    for c in worker_cores:
        set_freq(c, args.freq_high, dry_run)

    # Rank PIDs (optional)
    rank_pids: List[int] = []
    if args.pids:
        rank_pids = parse_pid_list(args.pids)
        print(f"[MON] Using explicit PIDs: count={len(rank_pids)}")
    elif args.binary:
        uid = None
        try:
            uid = os.getuid()
        except Exception:
            uid = None
        # discovery is best-effort and can be retried after job starts
        rank_pids = []
        print(f"[MON] Will attempt PID discovery for binary '{args.binary}' (np={args.np or 'unknown'})")
    else:
        print("[MON] No --pids or --binary given. Will do DVFS-only based on marker.")
    print()

    # State for critical-path detection during COMM
    prev_cpu_time = {}  # pid -> jiffies
    last_boost_pid: Optional[int] = None
    last_boost_core: Optional[int] = None

    # Thrash prevention
    last_freq_apply_t = 0.0

    # Phase tracking
    current_phase = "COMPUTE"
    phase_start = time.time()
    transitions = 0

    def ts_now() -> str:
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def maybe_apply_freqs(policy: str,
                          high_cores: List[int],
                          mid_cores: List[int],
                          low_cores: List[int],
                          data_bytes: int,
                          boost_pid: Optional[int],
                          boost_core: Optional[int]) -> None:
        nonlocal last_freq_apply_t
        now = time.time()
        if (now - last_freq_apply_t) < args.min_dwell_s:
            return

        # Apply frequencies
        for c in low_cores:
            set_freq(c, args.freq_low, dry_run)
        for c in mid_cores:
            set_freq(c, args.freq_mid, dry_run)
        for c in high_cores:
            set_freq(c, args.freq_high, dry_run)

        last_freq_apply_t = now

        if log_file:
            log_file.write(
                f"{now:.6f},{current_phase},{policy},{boost_core if boost_core is not None else ''},"
                f"{boost_pid if boost_pid is not None else ''},{rank0_core},"
                f"{args.freq_high},{args.freq_mid},{args.freq_low},{data_bytes}\n"
            )
            log_file.flush()

    def apply_sched_policy(phase: str) -> None:
        """
        Apply renice/ionice policies to rank pids (best effort).
        """
        if not rank_pids:
            return

        if not args.disable_renice and phase in ("IO", "COMMUNICATION"):
            for pid in rank_pids:
                renice_pid(pid, args.nice_iocomm)

        if not args.disable_ionice:
            if phase == "IO":
                for pid in rank_pids:
                    ionice_pid(pid, args.ionice_io_class, args.ionice_io_level)
            elif phase == "COMPUTE":
                for pid in rank_pids:
                    ionice_pid(pid, args.ionice_compute_class, args.ionice_compute_level)
            elif phase == "COMMUNICATION":
                # treat like IO-ish (best effort low), but keep it mild
                for pid in rank_pids:
                    ionice_pid( pid, args.ionice_io_class, min(7, max(0, args.ionice_io_level)) )

    def refresh_pids_if_needed() -> None:
        """
        Best-effort pid discovery retry when user provides --binary.
        """
        nonlocal rank_pids
        if rank_pids or (not args.binary):
            return

        want = args.np if args.np and args.np > 0 else 0
        uid = None
        try:
            uid = os.getuid()
        except Exception:
            uid = None

        found = discover_rank_pids(worker_cores, args.binary, want, uid)
        if found:
            rank_pids = found
            print(f"[{ts_now()}] [MON] Discovered rank PIDs: count={len(rank_pids)} -> {rank_pids}")

    def choose_comm_boost() -> Tuple[Optional[int], Optional[int]]:
        """
        Pick the pid with largest delta (utime+stime) since last sample.
        Returns (boost_pid, boost_core).
        """
        if not rank_pids:
            return (None, None)

        best_pid = None
        best_delta = -1
        for pid in rank_pids:
            cur = read_cpu_time_jiffies(pid)
            if cur is None:
                continue
            prev = prev_cpu_time.get(pid, cur)
            d = cur - prev
            prev_cpu_time[pid] = cur
            if d > best_delta:
                best_delta = d
                best_pid = pid

        if best_pid is None:
            return (None, None)
        core = pid_to_core(best_pid)
        return (best_pid, core)

    # Main loop
    try:
        print("[MON] Monitoring started.")
        while True:
            # if binary provided, try to discover pids when possible
            refresh_pids_if_needed()

            content = read_phase_marker(marker_path)
            new_phase, data_bytes = parse_phase(content)

            # Adaptive controller sleep: event-driven if possible, else phase-based polling
            sleep_s = args.poll_fast if current_phase == "COMPUTE" else args.poll_slow

            # Transition detection
            if new_phase != current_phase:
                now = time.time()
                prev_duration = now - phase_start
                transitions += 1
                print(f"[{ts_now()}] Phase transition: {current_phase} -> {new_phase} (was {prev_duration:.3f}s)")

                current_phase = new_phase
                phase_start = now

                # Apply scheduling knobs immediately on phase change (best effort)
                apply_sched_policy(current_phase)

                # COMM: reset cpu time baselines so deltas are meaningful
                if current_phase == "COMMUNICATION":
                    prev_cpu_time = {}
                    for pid in rank_pids:
                        v = read_cpu_time_jiffies(pid)
                        if v is not None:
                            prev_cpu_time[pid] = v

            # Phase-duration gating: only enforce IO/COMM DVFS if phase likely lasts long enough
            phase_elapsed = time.time() - phase_start
            allow_act = (phase_elapsed >= args.min_phase_s)

            last_boost_pid = None
            last_boost_core = None

            if current_phase == "COMPUTE":
                if allow_act:
                    # all high
                    maybe_apply_freqs(
                        policy="compute_all_high",
                        high_cores=worker_cores,
                        mid_cores=[],
                        low_cores=[],
                        data_bytes=data_bytes,
                        boost_pid=None,
                        boost_core=None
                    )

            elif current_phase == "IO":
                if allow_act:
                    # all low
                    maybe_apply_freqs(
                        policy="io_all_low",
                        high_cores=[],
                        mid_cores=[],
                        low_cores=worker_cores,
                        data_bytes=data_bytes,
                        boost_pid=None,
                        boost_core=None
                    )

            elif current_phase == "COMMUNICATION":
                if allow_act:
                    # choose critical pid/core
                    boost_pid, boost_core = choose_comm_boost()

                    # fallback to provided rank0 core if we can't detect
                    if boost_core is None or boost_core not in worker_cores:
                        boost_core = rank0_core
                        boost_pid = None

                    last_boost_pid = boost_pid
                    last_boost_core = boost_core

                    # Policy:
                    # - boost core HIGH
                    # - park others LOW
                    highs = [boost_core]
                    lows = [c for c in worker_cores if c != boost_core]

                    maybe_apply_freqs(
                        policy="comm_boost_one_low_rest",
                        high_cores=highs,
                        mid_cores=[],
                        low_cores=lows,
                        data_bytes=data_bytes,
                        boost_pid=boost_pid,
                        boost_core=boost_core
                    )

                    if data_bytes > 0:
                        # print occasionally (not every loop)
                        if int(time.time() * 10) % 10 == 0:
                            mb = data_bytes / (1024 * 1024)
                            bp = f"pid={boost_pid}" if boost_pid else "pid=?"
                            print(f"[{ts_now()}] COMM active_core={boost_core} {bp}, payload={mb:.2f} MB")

            # Wait (inotify if enabled/available; otherwise sleep)
            if watcher is not None:
                watcher.wait_for_change(sleep_s)
            else:
                time.sleep(sleep_s)

    except KeyboardInterrupt:
        print(f"\n[MON] Stopped. Total transitions: {transitions}")
        print("[MON] Restoring all worker cores to HIGH frequency...")
        for c in worker_cores:
            set_freq(c, args.freq_high, dry_run)
        if log_file:
            log_file.close()


if __name__ == "__main__":
    main()
