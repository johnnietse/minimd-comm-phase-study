#!/usr/bin/env python3
import os
import time
import mmap
import ctypes
import argparse

# Phase constants matching dashboard.py
PHASE_COMPUTE      = 0
PHASE_COMMUNICATE  = 1
PHASE_EXCHANGE     = 2
PHASE_BORDERS      = 3
PHASE_REVERSE      = 4
PHASE_IO           = 5
PHASE_DONE         = 8

PHASE_MAGIC = 0x50485331
MAX_PHASE_SLOTS = 64

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

def main():
    parser = argparse.ArgumentParser(description="Bridge phase_marker.txt -> Shared Memory Hint File")
    parser.add_argument("--marker", default="phase_marker.txt", help="Input text file path")
    parser.add_argument("--hint-file", default="/dev/shm/minimd_phase_hints.bin", help="Output shared memory file")
    parser.add_argument("--ranks", type=int, default=16, help="Number of ranks to simulate in dashboard")
    args = parser.parse_args()

    size = ctypes.sizeof(PhaseTable)
    
    # Ensure hint file exists and is the right size
    if not os.path.exists(args.hint_file):
        with open(args.hint_file, "wb") as f:
            f.write(b"\0" * size)

    with open(args.hint_file, "r+b") as f:
        mm = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_WRITE)
        table = PhaseTable.from_buffer(mm)
        table.magic = PHASE_MAGIC
        table.nslots = args.ranks

        current_phase = PHASE_COMPUTE
        print(f"Bridge started.")
        print(f"Reading: {args.marker}")
        print(f"Writing: {args.hint_file} ({args.ranks} ranks)")
        print("Press Ctrl+C to stop.")

        try:
            while True:
                new_phase = current_phase
                if os.path.exists(args.marker):
                    try:
                        with open(args.marker, 'r') as mf:
                            content = mf.read().strip()
                        
                        if "COMM_START" in content: new_phase = PHASE_COMMUNICATE
                        elif "IO_START" in content: new_phase = PHASE_IO
                        elif "COMM_END" in content or "IO_END" in content or "COMPUTE_RESUME" in content:
                            new_phase = PHASE_COMPUTE
                        elif "DONE" in content: new_phase = PHASE_DONE
                    except Exception:
                        pass # avoid crash on race condition reading file

                if new_phase != current_phase:
                    t_ns = time.monotonic_ns()
                    for i in range(args.ranks):
                        slot = table.slots[i]
                        slot.seq += 1 # write start
                        slot.rank = i
                        slot.core = i
                        slot.phase = new_phase
                        slot.t_ns = t_ns
                        slot.seq += 1 # write end
                    current_phase = new_phase
                    print(f"Update: Phase -> {new_phase}")

                time.sleep(0.05)
        except KeyboardInterrupt:
            print("\nBridge stopped.")

if __name__ == "__main__":
    main()
