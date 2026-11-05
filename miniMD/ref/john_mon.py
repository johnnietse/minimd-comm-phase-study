#!/usr/bin/env python3
"""
Lightweight Communication Phase Frequency Controller for miniMD
===============================================================
Watches phase_marker.txt (or --marker path) and sets CPU frequency accordingly.

Key improvement:
  - Supports NON-SEQUENTIAL worker core allocations via --worker-cores
    Example: --worker-cores "2,4,6,10-13"

Still supports:
  - --workers N (legacy mode): assumes workers are cores 0..N-1
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime

# ============ CONFIGURATION ============
PHASE_MARKER = "phase_marker.txt"
POLL_INTERVAL = 0.05  # 50ms

# Default frequencies (in kHz as expected by cpufreq sysfs)
DEFAULT_FREQ_HIGH = 2400000   # 2.4 GHz (frnt115 max)
DEFAULT_FREQ_MID  = 1600000   # 1.6 GHz (currently unused by policy)
DEFAULT_FREQ_LOW  = 1200000   # 1.2 GHz

# Core constraints
RESERVED_CORE = 31  # HPC maintenance — never touch

# ============ HELPERS ============

def parse_core_list(s: str):
    """
    Parse "0-3,6,8-9" -> sorted unique list of ints.
    Also accepts "2,4,6,10-13".
    """
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

def get_cpuset_str_for_pid(pid: int):
    """
    Returns cpuset string like '0-3,6,8-9' for a pid using taskset -cp.
    If taskset isn't available or fails, returns None.
    """
    try:
        out = subprocess.check_output(["taskset", "-cp", str(pid)], text=True).strip()
        # format: "pid 1234's current affinity list: 0-3,6"
        if ":" in out:
            return out.split(":", 1)[1].strip()
    except Exception:
        return None
    return None

def pick_monitor_core(worker_cores):
    """
    Picks a monitor core automatically from THIS process' cpuset:
      - choose highest available core not in worker_cores
    Returns int monitor_core, or None if cannot pick.
    """
    cpuset = get_cpuset_str_for_pid(os.getpid())
    if not cpuset:
        return None

    avail = parse_core_list(cpuset)
    wset = set(worker_cores)
    candidates = [c for c in avail if c not in wset]

    if not candidates:
        return None
    return max(candidates)

def set_freq(core, freq, dry_run=False):
    """Set CPU frequency for a specific core (skips reserved core)."""
    if core == RESERVED_CORE:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_setspeed"
    if dry_run:
        return True
    try:
        with open(path, "w") as f:
            f.write(str(int(freq)))
        return True
    except Exception:
        return False

def set_governor(core, gov, dry_run=False):
    """Set CPU governor for a specific core (skips reserved core)."""
    if core == RESERVED_CORE:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
    if dry_run:
        return True
    try:
        with open(path, "w") as f:
            f.write(gov)
        return True
    except Exception:
        return False

def read_phase_marker(marker_path):
    try:
        if os.path.exists(marker_path):
            with open(marker_path, "r") as f:
                return f.read().strip()
    except Exception:
        pass
    return None

# ============ MAIN ============

def main():
    parser = argparse.ArgumentParser(
        description="Communication Phase Frequency Controller (supports non-sequential cores)"
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--workers", type=int,
                      help="LEGACY: number of worker cores (assumes cores 0..N-1)")
    mode.add_argument("--worker-cores", type=str,
                      help='Explicit worker core list, e.g. "2,4,6,10-13"')

    parser.add_argument("--freq-high", type=int, default=DEFAULT_FREQ_HIGH)
    parser.add_argument("--freq-mid", type=int, default=DEFAULT_FREQ_MID)
    parser.add_argument("--freq-low", type=int, default=DEFAULT_FREQ_LOW)

    parser.add_argument("--marker", type=str, default=PHASE_MARKER)
    parser.add_argument("--rank0-core", type=int, default=None,
                        help="Core ID for MPI rank 0. Default: first worker core.")

    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log", type=str, default=None)

    parser.add_argument("--suggest-monitor-core", action="store_true",
                        help="Print an auto-picked monitor core (from cpuset) and exit.")

    args = parser.parse_args()

    # Determine worker cores
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

    # rank0 core default
    rank0_core = args.rank0_core if args.rank0_core is not None else worker_cores[0]

    # Basic safety
    if rank0_core not in worker_cores:
        print(f"ERROR: rank0-core {rank0_core} is not in worker core list {worker_cores}")
        sys.exit(1)

    if RESERVED_CORE in worker_cores:
        print(f"ERROR: worker core list includes reserved core {RESERVED_CORE}. Remove it.")
        sys.exit(1)

    # Optional monitor core suggestion
    if args.suggest_monitor_core:
        mon = pick_monitor_core(worker_cores)
        if mon is None:
            print("NO_MONITOR_CORE_FOUND")
            sys.exit(2)
        print(mon)
        sys.exit(0)

    marker_path = args.marker
    dry_run = args.dry_run

    log_file = None
    if args.log:
        log_file = open(args.log, "w")
        log_file.write("timestamp,phase,rank0_freq,other_freq,data_bytes\n")

    # Informational banner
    mon_guess = pick_monitor_core(worker_cores)
    print("[COMM CTRL] Communication Phase Frequency Controller")
    print(f"[COMM CTRL] Worker cores: {worker_cores}")
    print(f"[COMM CTRL] Rank 0 core: {rank0_core}")
    if mon_guess is not None:
        print(f"[COMM CTRL] (FYI) Suggested monitor core (from your cpuset): {mon_guess}")
    print(f"[COMM CTRL] Reserved core: {RESERVED_CORE} (never touched)")
    print(f"[COMM CTRL] Freq HIGH: {args.freq_high/1000:.0f} MHz")
    print(f"[COMM CTRL] Freq MID:  {args.freq_mid/1000:.0f} MHz (unused by current policy)")
    print(f"[COMM CTRL] Freq LOW:  {args.freq_low/1000:.0f} MHz")
    print(f"[COMM CTRL] Marker: {marker_path}")
    print(f"[COMM CTRL] Dry run: {dry_run}")
    print(f"[COMM CTRL] Polling every {POLL_INTERVAL*1000:.0f}ms")
    print()

    # Set worker cores to userspace governor
    print(f"[COMM CTRL] Setting worker cores to 'userspace' governor...")
    ok = 0
    for c in worker_cores:
        if set_governor(c, "userspace", dry_run):
            ok += 1
    print(f"[COMM CTRL] Set governor on {ok}/{len(worker_cores)} worker cores")
    if ok == 0 and not dry_run:
        print("[COMM CTRL] WARNING: governor writes failed on all cores (permission/governor issue).")

    # Start in compute mode: all HIGH
    print("[COMM CTRL] Setting all worker cores to HIGH frequency (compute mode)...")
    for c in worker_cores:
        set_freq(c, args.freq_high, dry_run)

    current_phase = "COMPUTE"
    phase_start = time.time()
    transition_count = 0

    print("[COMM CTRL] Monitoring started. Watching phase marker...")
    print()

    try:
        while True:
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

            if new_phase != current_phase:
                now = time.time()
                duration = now - phase_start
                transition_count += 1

                # Extract optional bytes
                data_bytes = 0
                if content and content.startswith("COMM_START"):
                    parts = content.split()
                    if len(parts) >= 2:
                        try:
                            data_bytes = int(parts[1])
                        except ValueError:
                            data_bytes = 0

                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f"[{ts}] Phase transition: {current_phase} -> {new_phase} (was {duration:.3f}s)")

                # Apply policy + compute what we actually set (for correct logging)
                if new_phase == "COMMUNICATION":
                    # Rank0 high, others low
                    set_freq(rank0_core, args.freq_high, dry_run)
                    for c in worker_cores:
                        if c != rank0_core:
                            set_freq(c, args.freq_low, dry_run)
                    print(f"  -> Core {rank0_core}: {args.freq_high/1000:.0f} MHz, "
                          f"others: {args.freq_low/1000:.0f} MHz")
                    if data_bytes > 0:
                        print(f"  -> Data to transfer: {data_bytes/(1024*1024):.2f} MB")
                    r0 = args.freq_high
                    oth = args.freq_low

                elif new_phase == "IO":
                    for c in worker_cores:
                        set_freq(c, args.freq_low, dry_run)
                    print(f"  -> All workers: {args.freq_low/1000:.0f} MHz (I/O)")
                    r0 = args.freq_low
                    oth = args.freq_low

                else:  # COMPUTE
                    for c in worker_cores:
                        set_freq(c, args.freq_high, dry_run)
                    print(f"  -> All workers: {args.freq_high/1000:.0f} MHz (compute)")
                    r0 = args.freq_high
                    oth = args.freq_high

                if log_file:
                    log_file.write(f"{now},{new_phase},{r0},{oth},{data_bytes}\n")
                    log_file.flush()

                current_phase = new_phase
                phase_start = now

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[COMM CTRL] Stopped. Total transitions: {transition_count}")
        print(f"[COMM CTRL] Restoring all worker cores to HIGH frequency...")
        for c in worker_cores:
            set_freq(c, args.freq_high, dry_run)
        if log_file:
            log_file.close()

if __name__ == "__main__":
    main()
