#!/usr/bin/env python3
import os
import time
import argparse
import sys
import subprocess

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# --- TARGET APPLICATION ---
APP_NAME = "miniMD_openmpi"

# --- TIMING ---
LOOP_SLEEP_SEC = 0.5 

# --- FREQUENCY MAPPING ---
# Logic: 0-33% -> MIN | 34-66% -> MID | >66% -> MAX
UTIL_LOW_LIMIT = 33.0
UTIL_HIGH_LIMIT = 66.0

FREQ_MAX = 2000000 
FREQ_MID = 1600000
FREQ_MIN = 1200000

# ==============================================================================
# 1. PROCESS WATCHDOG (AUTO-EXIT LOGIC)
# ==============================================================================

def get_target_pids():
    """Finds PIDs for the target application to know if it's running."""
    try:
        # pgrep is cheap enough to run once at startup
        out = subprocess.check_output(
            ["pgrep", "-u", os.environ.get("USER"), "-f", APP_NAME],
            text=True
        ).strip()
        if out:
            return [int(x) for x in out.split() if x.isdigit()]
    except:
        pass
    return []

def are_pids_alive(pids):
    """Checks if any of the monitored PIDs are still alive (Zero Overhead)."""
    for pid in pids:
        try:
            # Signal 0 does not kill the process; it just checks access/existence
            os.kill(pid, 0)
            return True # At least one rank is still alive
        except OSError:
            pass
    return False

# ==============================================================================
# 2. CORE UTILIZATION READER
# ==============================================================================

class CoreUtilMonitor:
    def __init__(self, monitored_cores):
        self.cores = set(monitored_cores)
        self.prev_totals = {} 
        self.prev_idles = {} 
        self.f = None
        
        try:
            self.f = open("/proc/stat", "r")
        except Exception as e:
            print(f"[ERROR] Could not open /proc/stat: {e}")
            sys.exit(1)

    def get_utilization(self):
        self.f.seek(0)
        results = {}
        
        for line in self.f:
            if not line.startswith("cpu"): 
                if line.startswith("intr"): break 
                continue
            
            parts = line.split()
            cpu_label = parts[0]
            if len(cpu_label) == 3: continue 
            
            core_id = int(cpu_label[3:])
            if core_id not in self.cores: continue
            
            idle = int(parts[4]) + int(parts[5])
            total = sum(int(x) for x in parts[1:])
            
            if core_id in self.prev_totals:
                d_total = total - self.prev_totals[core_id]
                d_idle = idle - self.prev_idles[core_id]
                
                if d_total > 0:
                    util_pct = (1.0 - (d_idle / d_total)) * 100.0
                    results[core_id] = util_pct
                else:
                    results[core_id] = 0.0
            
            self.prev_totals[core_id] = total
            self.prev_idles[core_id] = idle
            
        return results

    def close(self):
        if self.f: self.f.close()

# ==============================================================================
# 3. CONTROLLER
# ==============================================================================



class DirectFrequencyController:
    def __init__(self, allowed_cores, monitor_only=False):
        self.handles = {}
        self.last_freq = {} 
        self.monitor_only = monitor_only
        self.cores = allowed_cores
        
        if not self.monitor_only:
            print("[INFO] Setting cores to 'userspace' governor...")
            for c in allowed_cores:
                try:
                    # --- STEP 1: FORCE GOVERNOR TO USERSPACE ---
                    # We must close this file immediately after writing
                    gov_path = f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor"
                    with open(gov_path, 'w') as f_gov:
                        f_gov.write("userspace")
                    
                    # --- STEP 2: OPEN SPEED CONTROL HANDLE ---
                    # Now that governor is userspace, setspeed should be writable
                    speed_path = f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed"
                    f_speed = open(speed_path, 'w')
                    
                    self.handles[c] = f_speed
                    self.last_freq[c] = 0
                except PermissionError:
                    print(f"[ERROR] Permission denied on Core {c}. Did you run with sudo?")
                except FileNotFoundError:
                    print(f"[WARN] Core {c} interface not found. (Is cpufreq loaded?)")
                except Exception as e:
                    print(f"[WARN] Failed to init core {c}: {e}")

    def set_freq(self, core, freq):
        if self.monitor_only: return
        
        # Only write if we have a handle AND the value has changed
        if core in self.handles and self.last_freq.get(core) != freq:
            try:
                f = self.handles[core]
                f.seek(0)
                f.write(str(freq))
                f.flush()
                self.last_freq[core] = freq
            except OSError as e:
                # Sometimes writing an invalid freq throws OSError
                pass
    
    def restore_governor(self, default_gov="performance"):
        """Restores the original governor so cores aren't stuck manually."""
        if self.monitor_only: return
        print(f"[INFO] Restoring cores to '{default_gov}' governor...")
        for c in self.cores:
            try:
                with open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor", 'w') as f_gov:
                    f_gov.write(default_gov)
            except:
                pass

    def close(self):
        # Close file handles
        for h in self.handles.values(): 
            try: h.close()
            except: pass
        
        # Optional: Auto-restore governor on exit
        # self.restore_governor()

# ==============================================================================
# 4. MAIN LOOP
# ==============================================================================

def run_monitor(cores, heartbeat=False, monitor_only=False):
    util_monitor = CoreUtilMonitor(cores)
    controller = DirectFrequencyController(cores, monitor_only=monitor_only)
    
    mode = "PASSIVE (Monitor Only)" if monitor_only else "ACTIVE (Freq Control)"
    if heartbeat:
        print(f"[INFO] Monitor Started. Mode: {mode}")

    # --- PHASE 1: WAIT FOR APP TO START ---
    target_pids = []
    wait_cycles = 0
    # Wait up to 10 seconds for miniMD to appear
    while not target_pids:
        target_pids = get_target_pids()
        if not target_pids:
            time.sleep(0.1)
            wait_cycles += 1
            if wait_cycles > 100: 
                print("[INFO] App never started. Exiting.")
                return

    if heartbeat: print(f"[INFO] Attached to {len(target_pids)} processes.")

    # --- PHASE 2: MONITOR LOOP ---
    try:
        # Prime the monitor
        util_monitor.get_utilization()
        time.sleep(0.1)

        while True:
            # A. Check Liveness (Auto-Exit Condition)
            if not are_pids_alive(target_pids):
                if heartbeat: print("[INFO] App finished. Exiting.")
                break

            # B. Sleep Phase
            time.sleep(LOOP_SLEEP_SEC)
            
            # C. Read & Act
            util_data = util_monitor.get_utilization()
            
            total_util = 0
            count = 0
            
            # Dictionary to track how many cores are in each state
            bucket_counts = {"LOW": 0, "MID": 0, "HIGH": 0}
           
            
            for core, util in util_data.items():
                
                # --- LOGIC BUCKETS ---
                if util <= UTIL_LOW_LIMIT:
                    freq = FREQ_MIN
                    bucket = "LOW"
                    io_bound += 1
                elif util <= UTIL_HIGH_LIMIT:
                    freq = FREQ_MID
                    bucket = "MID"
                else:
                    freq = FREQ_MAX
                    bucket = "HIGH"
                # ---------------------

                # Apply Frequency
                controller.set_freq(core, freq)
                
                # Update Stats
                total_util += util
                count += 1
                bucket_counts[bucket] += 1
            
            if heartbeat and count > 0:
                avg_util = total_util / count
                print(f"Avg Util: {avg_util:.1f}% | {bucket_counts}")
            


    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:

        util_monitor.close()
        # controller.reset_max() # Reset hardware before leaving
        controller.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, default="0-30")
    parser.add_argument('--heartbeat', action='store_true')
    parser.add_argument('--monitor-only', action='store_true')
    args = parser.parse_args()
    
    cores = []
    for part in args.cores.split(','):
        if '-' in part:
            s, e = map(int, part.split('-'))
            cores.extend(range(s, e+1))
        else:
            cores.append(int(part))
            
    run_monitor(cores, heartbeat=args.heartbeat, monitor_only=args.monitor_only)
