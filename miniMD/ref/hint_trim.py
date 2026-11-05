# ---------------------- FREQUENCY CONTROLLER ----------------------

class DirectFrequencyController:
    def __init__(self, cores: List[int]):
        self.cores = cores
        self.handles = {}
        self.current_freqs = {} # Cache to avoid redundant writes
        
        # Open sysfs files for writing
        # We need to open one for EACH core because they might be independent
        for c in cores:
            path = f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_setspeed"
            try:
                # Open with buffering=0 for immediate writes, or standard text
                self.handles[c] = open(path, 'w')
                self.current_freqs[c] = -1
            except OSError:
                print(f"[WARN] Could not open frequency control for Core {c}. Are you root?")

    def update(self, core_data_list: List[CoreData]):
        """
        Apply heuristics per-core and set frequency.
        """
        for data in core_data_list:
            cid = data.core_id
            if cid not in self.handles: continue
            
            # --- THE HEURISTIC LOGIC ---
            
            # 1. IDLE / BLOCKED CHECK
            # If the core is doing almost nothing (low cycles), save power.
            # 1.2GHz * 0.05s = 60M cycles. If < 1M, it's barely active.
            if data.util < 1000000: 
                target = FREQ_MIN
                
            # 2. SPIN DETECTION (The "Busy Wait" Trap)
            # High IPC but no memory access = Spinning on a lock
            elif data.is_spinning:
                target = FREQ_MIN
                
            # 3. MEMORY BOUND
            # CPU is waiting on RAM. Higher clocks won't help.
            elif data.mpki > MPKI_MEM_THRESHOLD: 
                target = FREQ_MID
                
            # 4. COMPUTE BOUND
            # Doing real work. Race to sleep.
            else:
                target = FREQ_MAX
            
            # --- APPLY FREQUENCY ---
            # IO Optimization: Only write if changed
            if self.current_freqs[cid] != target:
                try:
                    h = self.handles[cid]
                    h.seek(0)
                    h.write(str(target))
                    h.flush()
                    self.current_freqs[cid] = target
                except OSError:
                    pass

    def close(self):
        for h in self.handles.values():
            h.close()

# ---------------------- MAIN MONITOR CLASS ----------------------

class NativeMonitor:
    def __init__(self):
        self.config = {
            'rapl_path': "/sys/class/powercap/intel-rapl:0/energy_uj"
        }
        
        self.collector = MetricsCollector(self.config)
        self.controller = DirectFrequencyController(MY_CORES)
        
        # Setup CSV
        self.csv_file = open(LOG_FILE, 'w', newline='')
        fieldnames = [
            'timestamp', 'phase', 
            'ipc', 'miss_rate', 'pkg_pwr', 'dram_pwr', 'net_rx', 
            'active_cores', 'ctx_switches', 'sync_var'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        
    def run(self):
        print(f"[INFO] Native Monitor Started on Cores: {MY_CORES}")
        print(f"[INFO] Log file: {LOG_FILE}")
        print("Press Ctrl+C to stop.")
        
        try:
            while True:
                # 1. Sample (Collector handles timing internally via dt)
                # We sleep briefly to prevent busy-waiting the python loop itself
                time.sleep(0.05) 
                
                # metrics = System Global Data (for logs)
                # core_data = Per-Core Data (for controller)
                metrics, core_data = self.collector.sample()
                
                # 2. Control
                self.controller.update(core_data)
                
                # 3. Log
                # We determine a "Dominant Phase" just for the log label
                phase_label = "MIXED"
                if metrics.ipc > 1.5: phase_label = "COMPUTE"
                elif metrics.pkg_power < 50: phase_label = "IDLE"
                
                self.writer.writerow({
                    'timestamp': f"{metrics.timestamp:.4f}",
                    'phase': phase_label,
                    'ipc': f"{metrics.ipc:.2f}",
                    'miss_rate': f"{metrics.miss_rate:.2f}",
                    'pkg_pwr': f"{metrics.pkg_power:.1f}",
                    'dram_pwr': f"{metrics.dram_power:.1f}",
                    'net_rx': f"{metrics.net_rx_mbps:.1f}",
                    'active_cores': metrics.active_ranks,
                    'ctx_switches': f"{metrics.ctx_switches:.0f}",
                    'sync_var': f"{metrics.sync_variance:.1f}"
                })
                
        except KeyboardInterrupt:
            print("\n[STOP] Stopping...")
        finally:
            self.controller.close()
            self.csv_file.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=str, default="0,1,2,3,4,5,6,7", 
                       help="Comma-separated list of cores (e.g. 0,2,4)")
    args = parser.parse_args()
    
    # Override Global Config based on arguments
    global MY_CORES
    MY_CORES = [int(x) for x in args.cores.split(',')]
    
    # Ensure userspace governor (Optional check, good for safety)
    if not os.path.exists("/sys/devices/system/cpu/cpu0/cpufreq/scaling_setspeed"):
        print("[WARN] 'scaling_setspeed' not found. Ensure governor is set to 'userspace'.")

    monitor = NativeMonitor()
    monitor.run()

if __name__ == "__main__":
    main()
