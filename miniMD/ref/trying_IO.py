#use commands like 
#mpirun --oversubscribe -np 4 ./miniMD_openmpi -i in.lj.miniMD -n 10000 -c 100

#!/usr/bin/env python3
import time
from pathlib import Path
import subprocess

# ========= CONFIG =========
PERF_INTERVAL = 1.0          # perf sampling interval (seconds)
IPC_IO_THRESHOLD = 0.4       # below → likely I/O wait
POWER_ACTIVE_W = 3.0         # above idle → system active
CSV_PATH = "storage_io_monitor.csv"
HEARTBEAT_SEC = 5

RAPL_PKG = Path("/sys/class/powercap/intel-rapl:0/energy_uj")
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

# =====================================================
#            STARTUP LOGIC
# =====================================================

print("[startup] storage-I/O MiniMD monitor starting...")

rapl_ok = RAPL_PKG.exists()
if rapl_ok:
    print(f"[startup] RAPL found at {RAPL_PKG}")
else:
    print("[startup] RAPL NOT found, power detection disabled")

with open(CSV_PATH, "w") as f:
    f.write("timestamp,ipc,energy_W,phase\n")

print(f"[startup] logging to {CSV_PATH}")

# =====================================================
#               PROCESS DETECTION
# =====================================================

def get_minimd_pids():
    try:
        out = subprocess.check_output(["pgrep", "-f", "miniMD"]).decode().strip()
        return [int(pid) for pid in out.split()] if out else []
    except subprocess.CalledProcessError:
        return []

# =====================================================
#                    RAPL POWER
# =====================================================

def read_rapl(prev_energy):
    if not rapl_ok:
        return 0.0, prev_energy

    try:
        energy = int(RAPL_PKG.read_text().strip())
    except:
        return 0.0, prev_energy

    if prev_energy is None:
        return 0.0, energy

    delta = max(0, energy - prev_energy)
    watts = delta / (1e6 * PERF_INTERVAL)
    return watts, energy

# =====================================================
#                  PERF IPC READER
# =====================================================

def safe_extract(line):
    token = line.split()[0].replace(",", "")
    return int(token) if token.isdigit() else 0

def read_ipc():
    cmd = [
        PERF, "stat",
        "-e", "cycles,instructions",
        "sleep", str(PERF_INTERVAL)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    cycles = instr = 0
    for line in result.stderr.splitlines():
        if "cycles" in line:
            cycles = safe_extract(line)
        elif "instructions" in line:
            instr = safe_extract(line)

    return instr / cycles if cycles > 0 else 0

# =====================================================
#            WAIT FOR MiniMD
# =====================================================

print("[startup] waiting for MiniMD...")

while not get_minimd_pids():
    time.sleep(1)

print("[run] MiniMD detected — monitoring storage I/O")

# =====================================================
#               MAIN LOOP
# =====================================================

prev_energy = None
last_hb = time.time()
start_time = time.time()

while True:

    pids = get_minimd_pids()
    if not pids:
        print("[stop] MiniMD finished.")
        break

    ipc = read_ipc()
    watts, prev_energy = read_rapl(prev_energy)

    # ---- phase classification ----
    if ipc < IPC_IO_THRESHOLD and watts > POWER_ACTIVE_W:
        phase = "CHECKPOINT_IO"
    elif ipc >= IPC_IO_THRESHOLD:
        phase = "COMPUTE"
    else:
        phase = "IDLE"

    timestamp = round(time.time() - start_time, 2)

    with open(CSV_PATH, "a") as f:
        f.write(f"{timestamp},{ipc:.3f},{watts:.3f},{phase}\n")

    print(
        f"[{timestamp:5.1f}s] "
        f"IPC={ipc:.2f} | Power={watts:.2f}W | Phase={phase}"
    )

    if time.time() - last_hb >= HEARTBEAT_SEC:
        print(
            f"[hb] ranks={len(pids)}, ipc={ipc:.2f}, watts={watts:.2f}, phase={phase}"
        )
        last_hb = time.time()
