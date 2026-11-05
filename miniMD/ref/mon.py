
#!/usr/bin/env python3
import os
import sys
import time
import mmap
import ctypes
import argparse

PHASE_COMPUTE      = 0
PHASE_COMMUNICATE  = 1
PHASE_EXCHANGE     = 2
PHASE_BORDERS      = 3
PHASE_REVERSE      = 4
PHASE_IO           = 5
PHASE_SYNTH_ACTIVE = 6
PHASE_SYNTH_WAIT   = 7
PHASE_DONE         = 8

PHASE_NAMES = {
    0: "COMPUTE",
    1: "COMMUNICATE",
    2: "EXCHANGE",
    3: "BORDERS",
    4: "REVERSE",
    5: "IO",
    6: "SYNTH_ACTIVE",
    7: "SYNTH_WAIT",
    8: "DONE",
}

PHASE_MAGIC = 0x50485331
MAX_PHASE_SLOTS = 64
RESERVED_CORE = 31
MONITOR_CORE = 30

class PhaseSlot(ctypes.Structure):
    _fields_ = [
        ("seq", ctypes.c_uint32),
        ("rank", ctypes.c_int32),
        ("core", ctypes.c_int32),
        ("phase", ctypes.c_uint32),
        ("t_ns", ctypes.c_uint64),
    ]

class PhaseTable(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("nslots", ctypes.c_uint32),
        ("slots", PhaseSlot * MAX_PHASE_SLOTS),
    ]

def set_governor(core, gov, dry_run=False):
    if core in (RESERVED_CORE, MONITOR_CORE) or core < 0:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
    if dry_run:
        print(f"[dry-run] cpu{core}: governor <- {gov}")
        return True
    try:
        with open(path, "w") as f:
            f.write(gov)
        return True
    except Exception:
        return False

def set_freq(core, freq, dry_run=False):
    if core in (RESERVED_CORE, MONITOR_CORE) or core < 0:
        return False
    path = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_setspeed"
    if dry_run:
        print(f"[dry-run] cpu{core}: setspeed <- {freq}")
        return True
    try:
        with open(path, "w") as f:
            f.write(str(freq))
        return True
    except Exception:
        return False

def apply_mode(core, mode, freq, cache, dry_run=False):
    prev = cache.get(core)
    desired = (mode, freq)
    if prev == desired:
        return

    if mode == "performance":
        set_governor(core, "performance", dry_run)
    elif mode == "userspace":
        set_governor(core, "userspace", dry_run)
        set_freq(core, freq, dry_run)

    cache[core] = desired

def snapshot_slot(slot):
    while True:
        seq1 = slot.seq
        if seq1 & 1:
            continue
        rank = slot.rank
        core = slot.core
        phase = slot.phase
        t_ns = slot.t_ns
        seq2 = slot.seq
        if seq1 == seq2 and not (seq2 & 1):
            return rank, core, phase, t_ns

def desired_policy(phase, age_ms, args):
    # Always restore compute-like phases immediately.
    if phase in (PHASE_COMPUTE, PHASE_SYNTH_ACTIVE, PHASE_DONE):
        return ("performance", None)

    # Long, clearly low-value phases.
    if phase in (PHASE_IO, PHASE_SYNTH_WAIT):
        if age_ms >= args.low_after_ms:
            return ("userspace", args.freq_low)
        return ("performance", None)

    # Regular internal MPI phases: only touch them if they persist.
    if phase in (PHASE_COMMUNICATE, PHASE_EXCHANGE, PHASE_BORDERS, PHASE_REVERSE):
        if age_ms >= args.mid_after_ms:
            return ("userspace", args.freq_mid)
        return ("performance", None)

    return ("performance", None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hint-file", default=os.environ.get("PHASE_HINT_PATH", "/dev/shm/minimd_phase_hints.bin"))
    ap.add_argument("--freq-low", type=int, default=1200000)
    ap.add_argument("--freq-mid", type=int, default=1600000)
    ap.add_argument("--poll-ms", type=float, default=2.0)
    ap.add_argument("--low-after-ms", type=float, default=2.0)
    ap.add_argument("--mid-after-ms", type=float, default=5.0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    size = ctypes.sizeof(PhaseTable)

    print(f"[PHASE MON] waiting for {args.hint_file}")
    while not os.path.exists(args.hint_file):
        time.sleep(0.05)

    with open(args.hint_file, "r+b") as f:
        mm = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_WRITE)
        table = PhaseTable.from_buffer(mm)

        while table.magic != PHASE_MAGIC or table.nslots == 0:
            time.sleep(0.01)

        print(f"[PHASE MON] attached: {table.nslots} slots")
        cache = {}
        seen_cores = set()

        try:
            while True:
                now_ns = time.monotonic_ns()

                for i in range(table.nslots):
                    rank, core, phase, t_ns = snapshot_slot(table.slots[i])

                    if core < 0 or core in (RESERVED_CORE, MONITOR_CORE):
                        continue

                    seen_cores.add(core)
                    age_ms = (now_ns - t_ns) / 1_000_000.0
                    mode, freq = desired_policy(phase, age_ms, args)
                    apply_mode(core, mode, freq, cache, args.dry_run)

                time.sleep(args.poll_ms / 1000.0)

        except KeyboardInterrupt:
            print("\n[PHASE MON] restoring seen worker cores to performance")
            for core in sorted(seen_cores):
                set_governor(core, "performance", args.dry_run)

if __name__ == "__main__":
    main()
