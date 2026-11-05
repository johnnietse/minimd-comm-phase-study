#!/usr/bin/env python3
import time
import os
from pathlib import Path
import subprocess

# ========= CONFIG =========
INTERVAL = 0.5                  
POWER_MARGIN_W = 1.5            # Lowered for better sensitivity on small nodes
STREAK_NEEDED = 2               
CSV_PATH = "parallel_monitor.csv"
HEARTBEAT_SEC = 5

TICKS_PER_SEC = 100 
TICKS_IN_INTERVAL = (TICKS_PER_SEC * INTERVAL) 
IPC_THRESHOLD = 0.15            

# Thresholds for thread classification
THREAD_APP_LIMIT = 0.25         # Thread is 'App' if > 25% of a core
THREAD_ACTIVE_LIMIT = 0.05      # Thread is 'Active Helper' if > 5% 
SMOOTHING = 0.3                 # How much weight to give the NEW sample (0 to 1)

PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
# ==========================

RAPL_PKG = Path("/sys/class/powercap/intel-rapl:0/energy_uj")
rapl_ok = RAPL_PKG.exists()

def get_minife_pids():
    pids = []
    for name in ("miniFE.x", "miniMD.x"): 
        try:
            out = subprocess.check_output(["pgrep", "-x", name]).decode().strip()
            if out: pids += [int(pid) for pid in out.split()]
        except: pass
    return pids

def read_rapl(prev_energy):
    if not rapl_ok: return 0.0, prev_energy
    try:
        energy = int(RAPL_PKG.read_text().strip())
        if prev_energy is None: return 0.0, energy
        delta_uj = energy - prev_energy
        watts = delta_uj / (1e6 * INTERVAL)
        return (watts if watts > 0 else 0.0), energy
    except: return 0.0, prev_energy

def read_perf_metrics(pids):
    if not pids: return 0.0
    try:
        pid_to_watch = pids[0]
        cmd = [PERF, "stat", "-e", "cycles,instructions", "-p", str(pid_to_watch), "--", "sleep", "0.05"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1.2)
        cycles = instr = 0
        for line in result.stderr.splitlines():
            parts = line.strip().split()
            if not parts: continue
            val = int(parts[0].replace(",", "")) if parts[0].replace(",", "").isdigit() else 0
            if "cycles" in line: cycles = val
            elif "instructions" in line: instr = val
        return instr / cycles if cycles > 1000 else 0.0
    except: return 0.0

# --- SUMMARY TRACKING ---
phase_stats = {"IDLE": 0, "SERIAL": 0, "PARALLEL": 0, "STALLED/WAIT": 0, "SYNC_OVERHEAD": 0}

print("[startup] monitor starting (Oversubscription-Aware)...")
prev_task_usage = {}
prev_energy = None
last_hb = time.time()
idle_baseline_w = None
last_phase = "UNKNOWN"
streak_counter = 0

smoothed_util = 0.0
smoothed_apps = 0.0

while True:
    pids = get_minife_pids()
    if pids: break
    time.sleep(0.5)

monitor_start = time.time()

try:
    while True:
        loop_start = time.time()
        pids = get_minife_pids()
        if not pids: break

        watts, prev_energy = read_rapl(prev_energy)
        ipc = read_perf_metrics(pids)
        
        if idle_baseline_w is None and watts > 10.0: idle_baseline_w = watts
        baseline_val = idle_baseline_w if idle_baseline_w else 68.0

        total_util = 0.0
        app_threads = 0
        active_helpers = 0
        current_task_usage = {}

        for pid in pids:
            task_dir = Path(f"/proc/{pid}/task")
            if not task_dir.exists(): continue
            for tid_dir in task_dir.iterdir():
                tid = tid_dir.name
                try:
                    with open(tid_dir / "stat") as f:
                        parts = f.read().split()
                        ticks = int(parts[13]) + int(parts[14])
                        current_task_usage[tid] = ticks
                        if tid in prev_task_usage:
                            util = (ticks - prev_task_usage[tid]) / TICKS_IN_INTERVAL
                            total_util += util
                            if util >= THREAD_APP_LIMIT: app_threads += 1
                            elif util >= THREAD_ACTIVE_LIMIT: active_helpers += 1
                except: continue
        prev_task_usage = current_task_usage
        
        # --- UPDATED CLASSIFICATION (Thread-First) ---
        is_computing = (watts > baseline_val + POWER_MARGIN_W) or (ipc > IPC_THRESHOLD)

        smoothed_util = (SMOOTHING * total_util) + (1 - SMOOTHING) * smoothed_util
        smoothed_apps = (SMOOTHING * app_threads) + (1 - SMOOTHING) * smoothed_apps

        # Round the smoothed values for logic
        effective_apps = round(smoothed_apps)
        effective_util = smoothed_util
        
        # --- SMOOTHED CLASSIFICATION ---
        if effective_util < 0.2:
            current_phase = "IDLE"
        elif effective_apps > 1:
            current_phase = "PARALLEL" if is_computing else "SYNC_OVERHEAD"
        elif effective_apps == 1 or effective_util <= 1.5:
            current_phase = "SERIAL" if is_computing else "STALLED/WAIT"
        else:
            current_phase = "PARALLEL" if is_computing else "SYNC_OVERHEAD"

        # --- STATS & STREAK ---
        phase_stats[current_phase] += INTERVAL
        
        if current_phase != last_phase:
            streak_counter += 1
            if streak_counter >= STREAK_NEEDED:
                t = round(time.time() - monitor_start, 2)
                print(f"[phase @ {t:6.2f}s] {current_phase:12s} | util={total_util:3.1f} | apps={app_threads} | helpers={active_helpers} | IPC={ipc:.2f}")
                last_phase = current_phase
                streak_counter = 0
        else: streak_counter = 0

        if time.time() - last_hb >= HEARTBEAT_SEC:
            print(f"[hb] util={total_util:.1f}, apps={app_threads}, active_helpers={active_helpers}, watts={watts:.1f}")
            last_hb = time.time()

        time.sleep(max(0, INTERVAL - (time.time() - loop_start)))

except KeyboardInterrupt:
    pass

# --- FINAL SUMMARY ---
print("\n" + "="*30)
print("   APPLICATION PHASE SUMMARY")
print("="*30)
total_monitored = sum(phase_stats.values())
if total_monitored > 0:
    for p, val in phase_stats.items():
        pct = (val / total_monitored) * 100
        print(f"{p:15s} : {val:6.1f}s ({pct:5.1f}%)")
print("="*30)
