#!/usr/bin/env python3
"""
File Size Verification for miniMD Communication Phase
=====================================================
Verifies the per-rank and total data sizes across different problem sizes
and MPI process counts.

Core Layout (32-core node):
  Core 31:    RESERVED (no permission)
  Core 30:    MONITOR  (frequency controller)
  Cores 0-N:  WORKERS  (1 MPI process = 1 core)
  Valid worker counts: 1, 2, 4, 8, 16, 30

The communication payload formula:
  per_rank_bytes = 96 + nlocal × (3×sizeof(double)×3 + sizeof(int))
                 = 96 + nlocal × 76   (when MMD_float=double, 8 bytes)

  total_bytes = per_rank_bytes × nprocs

Usage:
  python3 verify_file_sizes.py
  python3 verify_file_sizes.py --nprocs 8 --sizes 5,10,20,30,40,50
  python3 verify_file_sizes.py --all-configs     # show all valid nprocs
"""

import argparse

# Constants matching integrate.cpp
HEADER_BYTES = 96
SIZEOF_DOUBLE = 8
SIZEOF_INT = 4
STANDIN_DEFAULT_MB = 309

def atoms_per_unit_cell():
    """FCC lattice: 4 atoms per unit cell"""
    return 4

def total_atoms(nx, ny, nz):
    """Total atoms in the system"""
    return 4 * nx * ny * nz

def per_rank_nlocal(natoms, nprocs):
    """Approximate nlocal per rank (evenly distributed)"""
    return natoms // nprocs

def per_rank_data_bytes(nlocal):
    """
    Matches calculate_per_rank_data_bytes() in integrate.cpp:
      header_bytes = 96
      position_bytes = nlocal × 3 × sizeof(double)     (x, y, z)
      velocity_bytes = nlocal × 3 × sizeof(double)     (vx, vy, vz)
      force_bytes    = nlocal × 3 × sizeof(double)     (fx, fy, fz)
      type_bytes     = nlocal × sizeof(int)
      total = 96 + nlocal × (3×8×3 + 4) = 96 + nlocal × 76
    """
    return HEADER_BYTES + nlocal * (3 * SIZEOF_DOUBLE * 3 + SIZEOF_INT)

def format_bytes(b):
    """Human-readable byte size"""
    if b >= 1024 * 1024 * 1024:
        return f"{b / (1024**3):.2f} GB"
    elif b >= 1024 * 1024:
        return f"{b / (1024**2):.2f} MB"
    elif b >= 1024:
        return f"{b / 1024:.2f} KB"
    else:
        return f"{b} B"

VALID_WORKER_COUNTS = [1, 2, 4, 8, 16, 30]

def main():
    parser = argparse.ArgumentParser(description="Verify miniMD communication file sizes")
    parser.add_argument("--nprocs", type=int, default=16,
                        help="Number of MPI processes/worker cores (default: 16)")
    parser.add_argument("--sizes", type=str, default="5,10,20,30,40,50",
                        help="Comma-separated system sizes (default: 5,10,20,30,40,50)")
    parser.add_argument("--standin-mb", type=int, default=STANDIN_DEFAULT_MB,
                        help=f"Stand-in size in MB (default: {STANDIN_DEFAULT_MB})")
    parser.add_argument("--all-configs", action="store_true",
                        help=f"Show sizes for all valid worker counts: {VALID_WORKER_COUNTS}")
    args = parser.parse_args()

    nprocs_list = VALID_WORKER_COUNTS if args.all_configs else [args.nprocs]
    sizes = [int(s) for s in args.sizes.split(",")]
    standin_bytes = args.standin_mb * 1024 * 1024

    for nprocs in nprocs_list:
        print("=" * 90)
        print(f"  miniMD Communication Phase — File Size Verification")
        print(f"  MPI processes / worker cores: {nprocs}")
        print(f"  Core layout: cores 0-{nprocs-1} workers, core 30 monitor, core 31 reserved")
        print(f"  Stand-in default: {args.standin_mb} MB ({standin_bytes:,} bytes)")
        print("=" * 90)
        print()

    # Header
    print(f"{'Size':>6} | {'Atoms':>12} | {'nlocal':>10} | {'Per-Rank':>12} | "
          f"{'Total (all)':>14} | {'vs Stand-in':>12} | {'C Uses':>10}")
    print("-" * 90)

    for sz in sizes:
        natoms = total_atoms(sz, sz, sz)
        nlocal = per_rank_nlocal(natoms, nprocs)
        per_rank = per_rank_data_bytes(nlocal)
        total = per_rank * nprocs

        if total >= standin_bytes:
            vs_standin = f"+{format_bytes(total - standin_bytes)}"
            c_uses = format_bytes(total)
            c_label = "runtime"
        else:
            vs_standin = f"-{format_bytes(standin_bytes - total)}"
            c_uses = format_bytes(standin_bytes)
            c_label = "stand-in"

        print(f"{sz:>6} | {natoms:>12,} | {nlocal:>10,} | {format_bytes(per_rank):>12} | "
              f"{format_bytes(total):>14} | {vs_standin:>12} | {c_uses:>10} ({c_label})")

        print()
        print("=" * 90)
        print()

    # Detailed breakdown for default nprocs
    nprocs = nprocs_list[0]
    print(f"DETAIL — Example run (size=32, nprocs={nprocs}):")
    print()
    sz = 32
    natoms = total_atoms(sz, sz, sz)
    nlocal = per_rank_nlocal(natoms, nprocs)
    per_rank = per_rank_data_bytes(nlocal)
    total = per_rank * nprocs

    print(f"  System: {sz}×{sz}×{sz} = {natoms:,} atoms")
    print(f"  nlocal per rank: {natoms:,} / {nprocs} = {nlocal:,}")
    print()
    print(f"  Breakdown per rank:")
    print(f"    Header:     {HEADER_BYTES} bytes")
    print(f"    Positions:  {nlocal} × 3 × {SIZEOF_DOUBLE} = {nlocal * 3 * SIZEOF_DOUBLE:,} bytes")
    print(f"    Velocities: {nlocal} × 3 × {SIZEOF_DOUBLE} = {nlocal * 3 * SIZEOF_DOUBLE:,} bytes")
    print(f"    Forces:     {nlocal} × 3 × {SIZEOF_DOUBLE} = {nlocal * 3 * SIZEOF_DOUBLE:,} bytes")
    print(f"    Types:      {nlocal} × {SIZEOF_INT} = {nlocal * SIZEOF_INT:,} bytes")
    print(f"    ---")
    print(f"    Total:      {per_rank:,} bytes ({format_bytes(per_rank)})")
    print()
    print(f"  Total all ranks: {per_rank:,} × {nprocs} = {total:,} bytes ({format_bytes(total)})")
    print()

    if total < standin_bytes:
        print(f"  ⚠  Runtime total ({format_bytes(total)}) < stand-in ({args.standin_mb} MB)")
        print(f"     C code will use the STAND-IN value ({args.standin_mb} MB)")
    else:
        print(f"  ✓  Runtime total ({format_bytes(total)}) >= stand-in ({args.standin_mb} MB)")
        print(f"     C code will use the RUNTIME value ({format_bytes(total)})")

    print()

    # What size is needed to exceed 309 MB?
    # total >= standin => per_rank * nprocs >= standin
    # (96 + nlocal*76) * nprocs >= standin
    # nlocal >= (standin/nprocs - 96) / 76
    need_nlocal = (standin_bytes / nprocs - HEADER_BYTES) / 76
    need_natoms = need_nlocal * nprocs
    # natoms = 4 * sz^3 => sz = (natoms/4)^(1/3)
    need_sz = (need_natoms / 4) ** (1/3)

    print(f"  To exceed {args.standin_mb} MB stand-in with {nprocs} procs:")
    print(f"    Need nlocal >= {int(need_nlocal):,} per rank")
    print(f"    Need natoms >= {int(need_natoms):,} total")
    print(f"    Need --size >= {int(need_sz) + 1} (i.e., {int(need_sz)+1}×{int(need_sz)+1}×{int(need_sz)+1} unit cells)")
    print()

if __name__ == "__main__":
    main()
