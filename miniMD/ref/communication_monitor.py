#!/usr/bin/env python3
import subprocess
import time
import csv
import re
import os
import signal
from pathlib import Path

# ---------------- CONFIG ----------------
CMD = ["mpirun", "--oversubscribe", "-np", "32", "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = "communication_phase_monitor.csv"
SAMPLE_INTERVAL = 1.0
PERF = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"

# Communication phase thresholds
MPI_TIME_THRESHOLD = 0.3  # If >30% time spent in MPI, likely communication bound
LOW_IPC_THRESHOLD = 0.7   # Low IPC during communication
HIGH_MISS_THRESHOLD = 0.4  # High cache miss rate during communication
# ----------------------------------------

class CommunicationMonitor:
    def __init__(self):
        self.proc = None
        self.csv_file = None
        self.writer = None
        self.start_time = None
        
    def get_miniMD_pids(self):
        """Get PIDs of running miniMD processes"""
        pids = []
        try:
            result = subprocess.run(["pgrep", "-f", "miniMD"], 
                                  capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                pids = [int(pid) for pid in result.stdout.strip().split()]
        except Exception as e:
            print(f"[ERROR] Failed to get PIDs: {e}")
        return pids

    def read_energy(self):
        """Read CPU package energy (microjoules)"""
        try:
            with open(RAPL_PATH, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            print(f"[WARN] Could not read RAPL energy: {e}")
            return 0

    def parse_perf_metrics(self, perf_output):
        """Parse perf stat output for communication-relevant metrics"""
        metrics = {
            "cycles": 0, "instructions": 0, 
            "cache_misses": 0, "cache_references": 0,
            "LLC_load_misses": 0, "LLC_loads": 0,
            "branch_misses": 0, "branches": 0
        }
        
        for line in perf_output.splitlines():
            line = line.strip()
            # Handle different perf output formats
            if "cycles" in line and "GHz" not in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["cycles"] = val
            elif "instructions" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["instructions"] = val
            elif "cache-misses" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["cache_misses"] = val
            elif "cache-references" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["cache_references"] = val
            elif "LLC-load-misses" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["LLC_load_misses"] = val
            elif "LLC-loads" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["LLC_loads"] = val
            elif "branch-misses" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["branch_misses"] = val
            elif "branches" in line:
                val = self.extract_metric_value(line)
                if val > 0: metrics["branches"] = val
                
        return metrics

    def extract_metric_value(self, line):
        """Extract numeric value from perf stat line"""
        try:
            # Split and find the first numeric token
            parts = line.split()
            for part in parts:
                part = part.replace(',', '')
                if part.replace('.', '').isdigit():
                    return int(float(part))
        except:
            pass
        return 0

    def get_mpi_communication_metrics(self):
        """Use perf to track MPI-related events if available"""
        try:
            # Try to get MPI-specific metrics if perf supports it
            perf_cmd = [
                PERF, "stat", "-e",
                "cycles,instructions,cache-misses,cache-references,LLC-loads,LLC-load-misses",
                "sleep", str(SAMPLE_INTERVAL)
            ]
            
            result = subprocess.run(perf_cmd, capture_output=True, text=True, timeout=SAMPLE_INTERVAL + 2)
            return self.parse_perf_metrics(result.stderr)
        except Exception as e:
            print(f"[WARN] perf failed: {e}")
            return {}

    def analyze_communication_pattern(self, metrics):
        """Analyze if current phase is communication-bound"""
        if not metrics or metrics["cycles"] == 0:
            return "UNKNOWN", 0, 0, 0

        # Calculate key metrics
        ipc = metrics["instructions"] / metrics["cycles"] if metrics["cycles"] > 0 else 0
        cache_miss_rate = metrics["cache_misses"] / metrics["cache_references"] if metrics["cache_references"] > 0 else 0
        llc_miss_rate = metrics["LLC_load_misses"] / metrics["LLC_loads"] if metrics["LLC_loads"] > 0 else 0
        
        # Communication phase detection logic
        is_communication_phase = (
            ipc < LOW_IPC_THRESHOLD or 
            cache_miss_rate > HIGH_MISS_THRESHOLD or
            llc_miss_rate > HIGH_MISS_THRESHOLD
        )
        
        if is_communication_phase:
            phase = "COMMUNICATION_BOUND"
            # Estimate communication intensity (0-1 scale)
            comm_intensity = max(
                (LOW_IPC_THRESHOLD - ipc) / LOW_IPC_THRESHOLD,
                min(cache_miss_rate, llc_miss_rate or cache_miss_rate)
            )
        else:
            phase = "COMPUTE_BOUND"
            comm_intensity = 0.0
            
        return phase, ipc, cache_miss_rate, comm_intensity

    def setup_csv(self):
        """Initialize CSV logging"""
        self.csv_file = open(LOG_FILE, "w", newline='')
        fieldnames = [
            "timestamp", "phase", "ipc", "cache_miss_rate", 
            "llc_miss_rate", "energy_J", "comm_intensity",
            "branch_miss_rate"
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        print(f"[INFO] Logging communication data to {LOG_FILE}")

    def run_monitoring(self):
        """Main monitoring loop"""
        print(f"[INFO] Starting communication phase monitoring")
        print(f"[INFO] MiniMD command: {' '.join(CMD)}")
        
        # Setup CSV logging
        self.setup_csv()
        
        # Wait for miniMD to start
        print("[INFO] Waiting for miniMD to launch...")
        while not self.get_miniMD_pids():
            time.sleep(1)
        
        print("[INFO] miniMD detected, starting monitoring...")
        
        # Launch miniMD in background
        self.proc = subprocess.Popen(
            CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Monitoring variables
        energy_prev = self.read_energy()
        self.start_time = time.time()
        sample_count = 0
        
        try:
            while self.proc.poll() is None and self.get_miniMD_pids():
                # Take measurement
                sample_count += 1
                
                # Get hardware metrics
                metrics = self.get_mpi_communication_metrics()
                
                # Read energy
                energy_now = self.read_energy()
                delta_energy = max(0, energy_now - energy_prev) / 1e6  # µJ to J
                energy_prev = energy_now
                
                # Analyze communication pattern
                phase, ipc, cache_miss_rate, comm_intensity = self.analyze_communication_pattern(metrics)
                
                # Calculate additional metrics
                llc_miss_rate = (metrics.get("LLC_load_misses", 0) / 
                               metrics.get("LLC_loads", 1) if metrics.get("LLC_loads", 0) > 0 else 0)
                branch_miss_rate = (metrics.get("branch_misses", 0) / 
                                  metrics.get("branches", 1) if metrics.get("branches", 0) > 0 else 0)
                
                # Log data
                timestamp = time.time() - self.start_time
                row = {
                    "timestamp": round(timestamp, 2),
                    "phase": phase,
                    "ipc": round(ipc, 3),
                    "cache_miss_rate": round(cache_miss_rate, 3),
                    "llc_miss_rate": round(llc_miss_rate, 3),
                    "energy_J": round(delta_energy, 4),
                    "comm_intensity": round(comm_intensity, 3),
                    "branch_miss_rate": round(branch_miss_rate, 3)
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print status (every 5 samples for readability)
                if sample_count % 5 == 0:
                    print(f"[{timestamp:6.1f}s] Phase: {phase:15} "
                          f"IPC: {ipc:.2f} Miss: {cache_miss_rate:.2f} "
                          f"Comm: {comm_intensity:.1%} Energy: {delta_energy:.2f}J")
                
                # Brief pause between samples
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("\n[INFO] Monitoring interrupted by user")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
        finally:
            self.cleanup()
            
    def cleanup(self):
        """Cleanup resources"""
        if self.proc and self.proc.poll() is None:
            print("[INFO] Terminating miniMD...")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        
        if self.csv_file:
            self.csv_file.close()
            
        print(f"[INFO] Monitoring complete. Data saved to {LOG_FILE}")

def main():
    """Main entry point"""
    monitor = CommunicationMonitor()
    
    # Check if perf is available
    if not os.path.exists(PERF):
        print(f"[ERROR] perf not found at {PERF}")
        print("[ERROR] Please ensure perf is installed and accessible")
        return 1
    
    # Check if we can read energy
    if not os.path.exists(RAPL_PATH):
        print(f"[WARN] RAPL energy monitoring not available at {RAPL_PATH}")
        print("[WARN] Energy data will be zero")
    
    try:
        monitor.run_monitoring()
    except Exception as e:
        print(f"[FATAL] Failed to run monitoring: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
