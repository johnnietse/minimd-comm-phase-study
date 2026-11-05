#!/usr/bin/env python3
"""
Lightweight Communication Phase Frequency Controller for miniMD
===============================================================
Watches phase_marker.txt and sets CPU frequency accordingly.
Designed to run on a DEDICATED monitoring core (core 30).

Core Layout (32-core node):
  Core 31:  RESERVED  — HPC maintenance, no permission to change freq
  Core 30:  MONITOR   — this Python controller runs here
  Cores 0–N: WORKERS  — MPI processes, 1:1 core-binding

Valid worker counts: 1, 2, 4, 8, 16, 30
  (30 = max because core 30 = monitor, core 31 = reserved)

Strategy (per Dr. Grant's guidance):
  - I/O phase:    All worker cores at 1.2 GHz (I/O-bound, save power)
  - COMM phase:   Rank 0's core (core 0) at 2.0 GHz (active network),
                  all other worker cores at 1.2 GHz (blocked at MPI_Barrier)
  - COMPUTE phase: All worker cores at max frequency (CPU-bound)

Usage:
  # Run on monitor core 30 with 16 workers (cores 0-15):
  taskset -c 30 python3 comm_freq_controller.py --workers 16

  # 8 workers:
  taskset -c 30 python3 comm_freq_controller.py --workers 8

  # Max 30 workers:
  taskset -c 30 python3 comm_freq_controller.py --workers 30

  # Dry-run:
  taskset -c 30 python3 comm_freq_controller.py --workers 16 --dry-run
"""

import os
import sys
import time
import argparse
from datetime import datetime

# ============ CONFIGURATION ============
PHASE_MARKER = "phase_marker.txt"
POLL_INTERVAL = 0.05  # 50ms — negligible overhead

# Default frequencies (in kHz as expected by cpufreq sysfs)
DEFAULT_FREQ_HIGH = 2400000   # 2.4 GHz (frnt115 max)
DEFAULT_FREQ_MID  = 1600000   # 1.6 GHz
DEFAULT_FREQ_LOW  = 1200000   # 1.2 GHz

# Core constraints
RESERVED_CORE = 31  # HPC maintenance — no permission to change freq
MONITOR_CORE = 30   # This controller runs here
VALID_WORKER_COUNTS = [1, 2, 4, 8, 16, 30]

# ============ FREQUENCY CONTROL ============

def set_freq(core, freq, dry_run=False):
    """Set CPU frequency for a specific core (skips reserved core 31)"""
    if core == RESERVED_CORE:
        return False  # Never touch core 31
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_setspeed"
    if dry_run:
        return True
    try:
        with open(path, 'w') as f:
            f.write(str(freq))
        return True
    except PermissionError:
        return False
    except Exception:
        return False

def set_governor(core, gov, dry_run=False):
    """Set CPU governor for a specific core (skips reserved core 31)"""
    if core == RESERVED_CORE:
        return False  # Never touch core 31
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
    if dry_run:
        return True
    try:
        with open(path, 'w') as f:
            f.write(gov)
        return True
    except PermissionError:
        return False
    except Exception:
        return False

def read_phase_marker(marker_path):
    """Read the phase marker file"""
    try:
        if os.path.exists(marker_path):
            with open(marker_path, 'r') as f:
                content = f.read().strip()
            return content
    except:
        pass
    return None

# ============ MAIN CONTROLLER LOOP ============

def main():
    parser = argparse.ArgumentParser(
        description="Communication Phase Frequency Controller",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Core Layout (32-core node):
  Core 31:    RESERVED (HPC maintenance, no permission)
  Core 30:    MONITOR  (this controller)
  Cores 0-N:  WORKERS  (MPI processes, 1:1 core-binding)

Valid worker counts: 1, 2, 4, 8, 16, 30

Examples:
  taskset -c 30 python3 comm_freq_controller.py --workers 16
  taskset -c 30 python3 comm_freq_controller.py --workers 8 --dry-run
  taskset -c 30 python3 comm_freq_controller.py --workers 30 --log freq.csv
""")
    parser.add_argument("--workers", type=int, required=True,
                        help=f"Number of worker cores/MPI ranks. Valid: {VALID_WORKER_COUNTS}")
    parser.add_argument("--freq-high", type=int, default=DEFAULT_FREQ_HIGH,
                        help=f"High frequency in kHz (default: {DEFAULT_FREQ_HIGH})")
    parser.add_argument("--freq-mid", type=int, default=DEFAULT_FREQ_MID,
                        help=f"Mid frequency in kHz (default: {DEFAULT_FREQ_MID})")
    parser.add_argument("--freq-low", type=int, default=DEFAULT_FREQ_LOW,
                        help=f"Low frequency in kHz (default: {DEFAULT_FREQ_LOW})")
    parser.add_argument("--marker", type=str, default=PHASE_MARKER,
                        help=f"Path to phase marker file (default: {PHASE_MARKER})")
    parser.add_argument("--rank0-core", type=int, default=0,
                        help="Core ID for MPI rank 0 (default: 0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without changing frequencies")
    parser.add_argument("--log", type=str, default=None,
                        help="Log file for phase transitions")
    args = parser.parse_args()

    num_workers = args.workers
    if num_workers not in VALID_WORKER_COUNTS:
        print(f"ERROR: Invalid worker count: {num_workers}")
        print(f"Valid counts: {VALID_WORKER_COUNTS}")
        sys.exit(1)

    # Worker cores are 0 to num_workers-1
    worker_cores = list(range(num_workers))
    rank0_core = args.rank0_core
    marker_path = args.marker
    dry_run = args.dry_run

    log_file = None
    if args.log:
        log_file = open(args.log, 'w')
        log_file.write("timestamp,phase,rank0_freq,other_freq,data_bytes\n")

    print(f"[COMM CTRL] Communication Phase Frequency Controller")
    print(f"[COMM CTRL] Workers: {num_workers} (cores 0-{num_workers-1})")
    print(f"[COMM CTRL] Monitor: core {MONITOR_CORE}")
    print(f"[COMM CTRL] Reserved: core {RESERVED_CORE} (no permission)")
    print(f"[COMM CTRL] Rank 0 core: {rank0_core}")
    print(f"[COMM CTRL] Freq HIGH: {args.freq_high/1000:.0f} MHz")
    print(f"[COMM CTRL] Freq MID:  {args.freq_mid/1000:.0f} MHz")
    print(f"[COMM CTRL] Freq LOW:  {args.freq_low/1000:.0f} MHz")
    print(f"[COMM CTRL] Marker: {marker_path}")
    print(f"[COMM CTRL] Dry run: {dry_run}")
    print(f"[COMM CTRL] Polling every {POLL_INTERVAL*1000:.0f}ms")
    print()

    # Set worker cores to userspace governor
    print(f"[COMM CTRL] Setting worker cores 0-{num_workers-1} to 'userspace' governor...")
    success_count = 0
    for c in worker_cores:
        if set_governor(c, "userspace", dry_run):
            success_count += 1
    print(f"[COMM CTRL] Set governor on {success_count}/{len(worker_cores)} cores")

    # Start with all worker cores at HIGH (compute phase)
    print("[COMM CTRL] Setting all worker cores to HIGH frequency (compute mode)...")
    for c in worker_cores:
        set_freq(c, args.freq_high, dry_run)

    current_phase = "COMPUTE"
    phase_start = time.time()
    transition_count = 0

    print("[COMM CTRL] Monitoring started. Waiting for phase_marker.txt...")
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

                # Extract data bytes if present
                data_bytes = 0
                if content and "COMM_START" in content:
                    parts = content.split()
                    if len(parts) >= 2:
                        try:
                            data_bytes = int(parts[1])
                        except ValueError:
                            pass

                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f"[{timestamp}] Phase transition: {current_phase} -> {new_phase} "
                      f"(was {duration:.3f}s)")

                # Apply frequency policy
                if new_phase == "COMMUNICATION":
                    # Rank 0 core: HIGH frequency for active network
                    # All other worker cores: LOW frequency (blocked at MPI_Barrier, idle)
                    set_freq(rank0_core, args.freq_high, dry_run)
                    for c in worker_cores:
                        if c != rank0_core:
                            set_freq(c, args.freq_low, dry_run)
                    print(f"  -> Core {rank0_core}: {args.freq_high/1000:.0f} MHz (networking), "
                          f"cores 1-{num_workers-1}: {args.freq_low/1000:.0f} MHz (idle/blocked)")
                    if data_bytes > 0:
                        print(f"  -> Data to transfer: {data_bytes/(1024*1024):.2f} MB")

                elif new_phase == "IO":
                    # I/O: all worker cores at LOW frequency
                    for c in worker_cores:
                        set_freq(c, args.freq_low, dry_run)
                    print(f"  -> All {num_workers} worker cores: {args.freq_low/1000:.0f} MHz (I/O bound)")

                elif new_phase == "COMPUTE":
                    # Compute: all worker cores at HIGH frequency
                    for c in worker_cores:
                        set_freq(c, args.freq_high, dry_run)
                    print(f"  -> All {num_workers} worker cores: {args.freq_high/1000:.0f} MHz (CPU bound)")

                # Log transition
                if log_file:
                    log_file.write(f"{now},{new_phase},{args.freq_high},{args.freq_low},{data_bytes}\n")
                    log_file.flush()

                current_phase = new_phase
                phase_start = now

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[COMM CTRL] Stopped. Total transitions: {transition_count}")
        print(f"[COMM CTRL] Restoring all {num_workers} worker cores to HIGH frequency...")
        for c in worker_cores:
            set_freq(c, args.freq_high, dry_run)
        if log_file:
            log_file.close()

if __name__ == "__main__":
    main()
