#!/usr/bin/env python3
"""
Phase-based communication monitoring for miniMD
Authoritative phase detection using miniMD timers
Metric collection via PID-scoped perf
"""

import subprocess
import time
import csv
import re
import os
from pathlib import Path

# ---------------- CONFIG ----------------
MINIMD_CMD = [
    "mpirun", "--oversubscribe", "-np", "32",
    "./miniMD_openmpi", "-i", "in.lj.miniMD"
]

PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"

SAMPLE_INTERVAL = 0.5   # seconds
CSV_FILE = "communication_phase_monitor.csv"

# Phase thresholds (REPORTABLE)
COMM_THRESHOLD = 0.5
COMP_THRESHOLD = 0.2
# ---------------------------------------


# ---------- Utility Functions ----------

def read_energy_uj():
    try:
        with open(RAPL_PATH) as f:
            return int(f.read().strip())
    except Exception:
        return 0


def get_minimd_pids():
    result = subprocess.run(
        ["pgrep", "-f", "miniMD_openmpi"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return []
    return [pid for pid in result.stdout.split() if pid.isdigit()]


def run_perf_on_pid(pid, duration):
    perf_cmd = [
        PERF, "stat", "-p", pid,
        "-e", "cycles,instructions,cache-references,cache-misses",
        "sleep", str(duration)
    ]
    result = subprocess.run(
        perf_cmd, capture_output=True, text=True
    )
    return result.stderr


def parse_perf_output(text):
    metrics = {
        "cycles": 0,
        "instructions": 0,
        "cache_refs": 0,
        "cache_misses": 0
    }

    for line in text.splitlines():
        line = line.replace(",", "")
        tokens = line.split()
        if not tokens:
            continue
        try:
            value = int(float(tokens[0]))
        except:
            continue

        if "cycles" in line:
            metrics["cycles"] += value
        elif "instructions" in line:
            metrics["instructions"] += value
        elif "cache-references" in line:
            metrics["cache_refs"] += value
        elif "cache-misses" in line:
            metrics["cache_misses"] += value

    return metrics


def parse_minimd_summary(output_text):
    """
    Extract t_comm and t_total from miniMD output
    """
    pattern = re.compile(
        r"\s+\d+\s+\d+\s+\d+\s+\d+\s+"
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"
    )

    for line in output_text.splitlines():
        if "PERF_SUMMARY" in line:
            parts = line.split()
            t_total = float(parts[4])
            t_comm = float(parts[7])
            return t_comm, t_total

    return None, None


def classify_phase(mpi_ratio):
    if mpi_ratio >= COMM_THRESHOLD:
        return "COMMUNICATION"
    elif mpi_ratio <= COMP_THRESHOLD:
        return "COMPUTE"
    else:
        return "MIXED"


# ---------- Main Monitoring Logic ----------

def main():
    if not Path(PERF).exists():
        print("[ERROR] perf not found")
        return 1

    print("[INFO] Launching miniMD...")
    proc = subprocess.Popen(
        MINIMD_CMD,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    energy_prev = read_energy_uj()
    start_time = time.time()

    rows = []

    print("[INFO] Monitoring while miniMD runs...")

    while proc.poll() is None:
        pids = get_minimd_pids()
        if not pids:
            time.sleep(0.1)
            continue

        pid = pids[0]  # representative rank

        perf_text = run_perf_on_pid(pid, SAMPLE_INTERVAL)
        metrics = parse_perf_output(perf_text)

        energy_now = read_energy_uj()
        delta_energy = max(0, energy_now - energy_prev) / 1e6
        energy_prev = energy_now

        ipc = (
            metrics["instructions"] / metrics["cycles"]
            if metrics["cycles"] > 0 else 0.0
        )

        cache_miss_rate = (
            metrics["cache_misses"] / metrics["cache_refs"]
            if metrics["cache_refs"] > 0 else 0.0
        )

        timestamp = time.time() - start_time

        rows.append({
            "timestamp": round(timestamp, 2),
            "ipc": round(ipc, 3),
            "cache_miss_rate": round(cache_miss_rate, 3),
            "energy_J": round(delta_energy, 4)
        })

    stdout, stderr = proc.communicate()

    print("[INFO] miniMD completed, parsing phase information...")

    t_comm, t_total = parse_minimd_summary(stdout)

    if t_comm is None:
        print("[ERROR] Could not extract miniMD timers")
        return 1

    mpi_ratio = t_comm / t_total
    phase = classify_phase(mpi_ratio)

    print(f"[INFO] Detected phase: {phase}")
    print(f"[INFO] MPI time ratio: {mpi_ratio:.3f}")

    # Write CSV
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "phase",
            "mpi_time_ratio",
            "ipc",
            "cache_miss_rate",
            "energy_J"
        ])
        for r in rows:
            writer.writerow([
                r["timestamp"],
                phase,
                round(mpi_ratio, 3),
                r["ipc"],
                r["cache_miss_rate"],
                r["energy_J"]
            ])

    print(f"[INFO] Data written to {CSV_FILE}")
    return 0


if __name__ == "__main__":
    exit(main())
