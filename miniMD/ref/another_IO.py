#!/usr/bin/env python3
import os
import time
import subprocess
import threading
from pathlib import Path

# ========================
# CONFIG
# ========================
APP_EXEC = "./miniFE.x"

# TRUE LOCAL STORAGE (NODE-LOCAL) DIRECTORY
LOCAL_DIR = "/tmp/local_io_test"

LOGFILE = "true_local_storage_io_energy.csv"

RAPL_PKG = Path("/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj")
SAMPLE_INTERVAL = 0.002   # 2 ms = very high resolution

os.makedirs(LOCAL_DIR, exist_ok=True)
stop_sampling = False


# ========================
# HELPERS
# ========================
def read_rapl_pkg0():
    return int(RAPL_PKG.read_text().strip())


def sample_energy():
    """Sample RAPL energy ONLY during local /tmp I/O."""
    with open(LOGFILE, "w") as f:
        f.write("timestamp,energy_uj\n")
        while not stop_sampling:
            ts = time.time()
            e = read_rapl_pkg0()
            f.write(f"{ts},{e}\n")
            f.flush()
            time.sleep(SAMPLE_INTERVAL)


# ========================
# TRUE LOCAL I/O ONLY
# ========================
def run_true_local_io():
    global stop_sampling

    print("\n[STEP 1] Running MiniFE compute to generate matrix.dat...")
    subprocess.run(
        [
            "mpirun", "-n", "1", APP_EXEC,
            "--nx", "128", "--ny", "128", "--nz", "128",
            "--matrix-file", "matrix.dat"
        ],
        check=True
    )
    print("[STEP 1 DONE] matrix.dat created.\n")

    # Optional: clear page cache (sudo might not work)
    # subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=False)

    print("[STEP 2] Starting high-frequency RAPL sampling (local I/O only)...")
    sampler = threading.Thread(target=sample_energy)
    sampler.start()
    time.sleep(0.005)

    print("[STEP 3] Running MiniFE to write ONLY to /tmp (true local disk)...")
    subprocess.run(
        [
            "mpirun", "-n", "1", APP_EXEC,
            "--matrix-file", "matrix.dat",
            "--output", f"{LOCAL_DIR}/output.dat",
            "--log",    f"{LOCAL_DIR}/log.txt"
        ],
        check=True
    )

    print("[STEP 4] Stopping RAPL sampling...")
    stop_sampling = True
    sampler.join()

    print("\n[DONE] True-local I/O energy saved to:", LOGFILE)
    print("Compare this to remote I/O energy for real analysis.\n")


if __name__ == "__main__":
    run_true_local_io()
