#!/usr/bin/env python3
import time
import os
import sys
from pathlib import Path
import subprocess
import glob

# ========= CONFIG =========
INTERVAL = 0.5                  
POWER_MARGIN_W = 1.5            
STREAK_NEEDED = 2               
IPC_THRESHOLD = 1.6           
THREAD_APP_LIMIT = 0.25         
THREAD_ACTIVE_LIMIT = 0.05      
SMOOTHING = 0.5 
MISS_THRESHOLD = 0.30

PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
FREQ_MAX = "2000000"
FREQ_MIN = "1600000"
MONITOR_CORE = 0    # The core the monitor will be on

CPU_SETSPEED_FILES = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_setspeed"))
CPU_GOVERNOR_FILES = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"))
# ==========================

# Check for Probe Mode
IS_PROBE = "--probe" in sys.argv

# Pin the monitor to core 0
os.sched_setaffinity(0, {MONITOR_CORE})

# Open handles that will not close
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
rapl_ok = os.path.exists(RAPL_PATH)
rapl_file = open(RAPL_PATH, 'r') if rapl_ok else None

# pre-open all CPU frequency files
speed_paths = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_setspeed"))
gov_paths = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"))

speed_handles = [open(p, 'w') for p in speed_paths]
gov_handles = [open(p, 'w') for p in gov_paths]


def set_system_frequency(freq_str):
    start = time.time()
    for g, s in zip(gov_handles, speed_handles):
        try:
            g.write("userspace"); g.flush()
            s.write(freq_str); s.flush()
        except: continue
    return time.time() - start

def get_minife_pids():
    pids = []
    for name in ("miniFE.x", "miniMD.x"): 
        try:
            out = subprocess.check_output(["pgrep", "-x", name]).decode().strip()
            if out: pids += [int(pid) for pid in out.split()]
        except: pass
    return pids

def read_rapl(prev_energy):
    if not rapl_file: return 0
    rapl_file.seek(0)
    return int(rapl_file.read().strip())

def read_perf_metrics(pids):
    start = time.time()
    if not pids: return 0.0, 0.0, 0.0
    try:
        pid_to_watch = pids[0]
        events = "cycles,instructions,cache-references,cache-misses"
        cmd = [PERF, "stat", "-e", events, "-p", str(pid_to_watch), "--", "sleep", "0.01"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1.2)
        
        cycles = instr = refs = misses = 0
        for line in result.stderr.splitlines():
            parts = line.strip().split()
            if not parts: continue
            val = int(parts[0].replace(",", "")) if parts[0].replace(",", "").isdigit() else 0
            if "cycles" in line: cycles = val
            elif "instructions" in line: instr = val
            elif "cache-misses" in line: misses = val
            elif "cache-references" in line: refs = val

        ipc = instr / cycles if cycles > 1000 else 0.0
        miss_rate = misses / refs if refs > 1000 else 0.0
        return ipc, miss_rate, (time.time() - start)
    except: return 0.0, 0.0, 0.0

# Stats for overhead probe
op_times = {"rapl": [], "perf": [], "dvfs": [], "proc": []}

if not IS_PROBE:
    print("[startup] monitor starting (Waiting for App)...")
    while True:
        pids = get_minife_pids()
        if pids: break
        time.sleep(0.5)
else:
    print("[startup] monitor starting (PROBE MODE)...")
    pids = [1] # Dummy PID for probe

set_system_frequency(FREQ_MIN)
monitor_start = time.time()
prev_task_usage = {}
prev_energy = None
last_hb = time.time()
last_phase = "UNKNOWN"
streak_counter = 0
smoothed_util = smoothed_apps = 0.0

phase_stats = {"IDLE": 0, "SERIAL": 0, "PARALLEL": 0, "MEMORY_BOUND": 0, "STALLED/WAIT": 0, "SYNC_OVERHEAD": 0}

try:
    while True:
        loop_start = time.time()
        
        if not IS_PROBE:
            pids = get_minife_pids()
            if not pids: break
        
        watts, prev_energy, t_rapl = read_rapl(prev_energy)
        ipc, miss_rate, t_perf = read_perf_metrics(pids)
        
        t_proc_start = time.time()
        total_util = app_threads = active_helpers = 0
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
        t_proc = time.time() - t_proc_start

        smoothed_util = (SMOOTHING * total_util) + (1 - SMOOTHING) * smoothed_util
        eff_util = smoothed_util

        # --- CLASSIFICATION ---
        is_computing = (ipc > IPC_THRESHOLD)
        is_mem_sig = (ipc < IPC_THRESHOLD) and (miss_rate > MISS_THRESHOLD)
        
        if eff_util < 0.15: current_phase = "IDLE"
        elif eff_util > 0.4 and is_mem_sig: current_phase = "MEMORY_BOUND"
        elif total_util > 1.1 or app_threads > 1: current_phase = "PARALLEL" if is_computing else "SYNC_OVERHEAD"
        elif app_threads == 1 or eff_util > 0.4: current_phase = "SERIAL" if is_computing else "STALLED/WAIT"
        else: current_phase = "STALLED/WAIT"

        phase_stats[current_phase] += INTERVAL
        
        t_dvfs = 0
        if current_phase != last_phase:
            streak_counter += 1
            if streak_counter >= STREAK_NEEDED:
                target_f = FREQ_MAX if current_phase in ["PARALLEL", "SERIAL"] else FREQ_MIN
                t_dvfs = set_system_frequency(target_f)
                last_phase = current_phase
                streak_counter = 0

        # Log operation times for overhead analysis
        op_times["rapl"].append(t_rapl); op_times["perf"].append(t_perf); op_times["dvfs"].append(t_dvfs); op_times["proc"].append(t_proc)

        time.sleep(max(0, INTERVAL - (time.time() - loop_start)))

except KeyboardInterrupt:
    if IS_PROBE:
        print("\n" + "="*35)
        print("   MONITOR OPERATION OVERHEAD")
        print("="*35)
        for op, times in op_times.items():
            avg = sum(times)/len(times) if times else 0
            print(f"{op.upper():10s} : {avg*1000:6.2f} ms")
        print(f"TOTAL LOOP : {sum(sum(t) for t in op_times.values())/len(op_times['rapl'])*1000:6.2f} ms")
    
    set_system_frequency("performance")
