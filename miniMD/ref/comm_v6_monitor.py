#!/usr/bin/env python3
"""
Production Communication Phase Monitor for miniMD on Frontenac
Uses /proc-based metrics + RAPL for cluster-compatible monitoring
Focuses on detecting MPI communication patterns without requiring perf permissions
"""

import subprocess
import time
import csv
import os
import sys
from pathlib import Path
from collections import defaultdict

# ============ CONFIGURATION ============
CMD = ["mpirun", "--oversubscribe", "-np", "32", 
       "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = "comm_phase_monitoring.csv"
SAMPLE_INTERVAL = 0.2  # 200ms for better temporal resolution
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_DRAM_PATH = "/sys/class/powercap/intel-rapl:0:0/energy_uj"  # DRAM energy

# Communication phase thresholds (calibrated for miniMD MPI patterns)
# From miniMD output: t_comm=10.85s / t_total=11.23s = 96.6% in communication!
POWER_COMM_LOW = 150.0      # Communication phases often <200W (waiting)
POWER_COMPUTE_HIGH = 250.0  # Compute phases >250W
CPU_IDLE_THRESHOLD = 60.0   # >60% idle = likely waiting in MPI
VOLUNTARY_CTX_HIGH = 50     # High voluntary switches = MPI blocking calls

class CommPhaseMonitor:
    def __init__(self):
        self.proc = None
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.prev_dram_energy = None
        self.prev_proc_stats = {}
        self.has_dram_rapl = os.path.exists(RAPL_DRAM_PATH)
        
    def get_miniMD_pids(self):
        """Get PIDs of running miniMD processes"""
        try:
            result = subprocess.run(["pgrep", "-f", "miniMD"],
                                  capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout.strip():
                return [int(pid) for pid in result.stdout.strip().split()]
        except Exception as e:
            pass
        return []
    
    def read_rapl_energy(self, path):
        """Read RAPL energy counter (microjoules)"""
        try:
            with open(path, "r") as f:
                return int(f.read().strip())
        except Exception:
            return 0
    
    def read_proc_stat(self):
        """
        Read /proc/stat for system-wide CPU metrics
        Returns: (user, nice, system, idle, iowait, irq, softirq, steal)
        """
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()  # First line is aggregate 'cpu'
                parts = line.split()
                if parts[0] == 'cpu':
                    return [int(x) for x in parts[1:9]]
        except Exception as e:
            print(f"[WARN] Failed to read /proc/stat: {e}")
        return [0] * 8
    
    def get_proc_stats_for_pids(self, pids):
        """
        Aggregate statistics from /proc/[pid]/stat for all miniMD processes
        Focus on context switches (voluntary = MPI blocking)
        """
        total_voluntary_ctxt = 0
        total_nonvoluntary_ctxt = 0
        
        for pid in pids:
            try:
                # Read context switches from /proc/[pid]/status
                with open(f"/proc/{pid}/status", "r") as f:
                    for line in f:
                        if line.startswith("voluntary_ctxt_switches"):
                            total_voluntary_ctxt += int(line.split()[1])
                        elif line.startswith("nonvoluntary_ctxt_switches"):
                            total_nonvoluntary_ctxt += int(line.split()[1])
            except (FileNotFoundError, ValueError, IndexError):
                continue
        
        return total_voluntary_ctxt, total_nonvoluntary_ctxt
    
    def calculate_cpu_usage(self, prev_stats, curr_stats):
        """
        Calculate CPU usage percentages from /proc/stat
        Returns: (user%, system%, idle%, iowait%)
        """
        if not prev_stats or len(prev_stats) < 8 or len(curr_stats) < 8:
            return 0, 0, 100, 0
        
        prev_total = sum(prev_stats)
        curr_total = sum(curr_stats)
        total_delta = curr_total - prev_total
        
        if total_delta == 0:
            return 0, 0, 100, 0
        
        # Calculate percentages
        user_pct = 100.0 * (curr_stats[0] - prev_stats[0]) / total_delta
        system_pct = 100.0 * (curr_stats[2] - prev_stats[2]) / total_delta
        idle_pct = 100.0 * (curr_stats[3] - prev_stats[3]) / total_delta
        iowait_pct = 100.0 * (curr_stats[4] - prev_stats[4]) / total_delta
        
        return user_pct, system_pct, idle_pct, iowait_pct
    
    def classify_comm_phase(self, power_W, dram_power_W, cpu_metrics, 
                           ctx_switch_rate, pids_count):
        """
        Classify execution phase using cluster-accessible metrics
        
        MPI Communication phases in miniMD are characterized by:
        1. LOW power consumption (CPUs waiting, not computing)
        2. HIGH CPU idle time (blocked in MPI calls)
        3. HIGH voluntary context switches (MPI_Wait, MPI_Recv blocking)
        4. LOWER DRAM power (not moving data through memory)
        5. MULTIPLE active processes (MPI ranks coordinating)
        
        From miniMD output analysis:
        - t_comm = 10.85s out of 11.23s total (96.6%!)
        - Communication dominates: exchange, borders, reverse_communicate
        """
        
        user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
        
        phase = "UNKNOWN"
        confidence = 0.0
        reasons = []
        
        # Calculate active CPU (inverse of idle)
        active_cpu = 100.0 - idle_pct
        
        # Indicator 1: Low power = waiting in MPI (not computing)
        if power_W < POWER_COMM_LOW and power_W > 50:  # Not idle, but not computing
            confidence += 0.35
            reasons.append(f"low_power({power_W:.0f}W)")
        
        # Indicator 2: High idle time = CPUs blocked in MPI
        if idle_pct > CPU_IDLE_THRESHOLD:
            confidence += 0.30
            reasons.append(f"high_idle({idle_pct:.0f}%)")
        
        # Indicator 3: High voluntary context switches = MPI blocking calls
        if ctx_switch_rate > VOLUNTARY_CTX_HIGH:
            confidence += 0.25
            reasons.append(f"mpi_blocking({ctx_switch_rate:.0f}/s)")
        
        # Indicator 4: Multiple processes active (MPI coordination)
        if pids_count >= 8:  # At least 8 ranks active
            confidence += 0.10
            reasons.append(f"multi_rank({pids_count})")
        
        # Phase classification
        if confidence >= 0.50:
            phase = "COMMUNICATION"
        elif power_W > POWER_COMPUTE_HIGH and active_cpu > 70:
            phase = "COMPUTE"
        elif iowait_pct > 10:
            phase = "IO_WAIT"
        elif idle_pct > 90:
            phase = "IDLE"
        else:
            phase = "MIXED"
        
        # Adjust confidence for non-communication phases
        if phase != "COMMUNICATION":
            confidence = 0.0
        
        details = {
            'phase': phase,
            'confidence': confidence,
            'power_W': power_W,
            'dram_power_W': dram_power_W,
            'cpu_user_pct': user_pct,
            'cpu_system_pct': system_pct,
            'cpu_idle_pct': idle_pct,
            'cpu_iowait_pct': iowait_pct,
            'voluntary_ctx_switches_per_sec': ctx_switch_rate,
            'active_mpi_ranks': pids_count,
            'reasons': ','.join(reasons) if reasons else 'none'
        }
        
        return phase, details
    
    def setup_csv(self):
        """Initialize CSV with cluster-accessible metrics"""
        self.csv_file = open(LOG_FILE, "w", newline='')
        fieldnames = [
            'timestamp',
            'phase',
            'confidence',
            'power_W',
            'dram_power_W',
            'cpu_user_pct',
            'cpu_system_pct', 
            'cpu_idle_pct',
            'cpu_iowait_pct',
            'voluntary_ctx_switches_per_sec',
            'active_mpi_ranks',
            'pkg_energy_J',
            'dram_energy_J',
            'reasons'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        print(f"[INFO] Logging to {LOG_FILE}")
        if self.has_dram_rapl:
            print("[INFO] DRAM RAPL available - tracking memory power")
        else:
            print("[INFO] DRAM RAPL not available - using pkg power only")
    
    def run_monitoring(self):
        """Main monitoring loop using /proc + RAPL"""
        print(f"[INFO] Starting production communication phase monitoring")
        print(f"[INFO] Command: {' '.join(CMD)}")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        print(f"[INFO] Detection: Power + CPU idle + context switches")
        
        self.setup_csv()
        
        # Wait for miniMD to start
        print("[INFO] Waiting for miniMD to launch...")
        while not self.get_miniMD_pids():
            time.sleep(0.5)
        
        print("[INFO] miniMD detected, starting monitoring...")
        
        # Launch miniMD (already started by user, just monitor)
        # Don't launch it ourselves - causes duplicate processes
        
        # Initialize energy counters
        self.prev_energy = self.read_rapl_energy(RAPL_PATH)
        if self.has_dram_rapl:
            self.prev_dram_energy = self.read_rapl_energy(RAPL_DRAM_PATH)
        
        # Initialize CPU stats
        self.prev_proc_stats = self.read_proc_stat()
        prev_ctx_switches = (0, 0)
        
        self.start_time = time.time()
        sample_count = 0
        
        try:
            while True:
                # Check if miniMD still running
                pids = self.get_miniMD_pids()
                if not pids:
                    print("[INFO] miniMD completed")
                    break
                
                sample_count += 1
                
                # Sleep for sampling interval
                time.sleep(SAMPLE_INTERVAL)
                
                # Read current energy
                energy_now = self.read_rapl_energy(RAPL_PATH)
                delta_pkg_energy = max(0, energy_now - self.prev_energy) / 1e6  # µJ to J
                self.prev_energy = energy_now
                
                # Calculate package power
                pkg_power_W = delta_pkg_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Read DRAM energy if available
                dram_power_W = 0
                delta_dram_energy = 0
                if self.has_dram_rapl:
                    dram_energy_now = self.read_rapl_energy(RAPL_DRAM_PATH)
                    delta_dram_energy = max(0, dram_energy_now - self.prev_dram_energy) / 1e6
                    self.prev_dram_energy = dram_energy_now
                    dram_power_W = delta_dram_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Read CPU utilization from /proc/stat
                curr_proc_stats = self.read_proc_stat()
                cpu_metrics = self.calculate_cpu_usage(self.prev_proc_stats, curr_proc_stats)
                self.prev_proc_stats = curr_proc_stats
                
                # Read context switches from processes
                curr_ctx_switches = self.get_proc_stats_for_pids(pids)
                voluntary_ctx_delta = curr_ctx_switches[0] - prev_ctx_switches[0]
                voluntary_ctx_rate = voluntary_ctx_delta / SAMPLE_INTERVAL
                prev_ctx_switches = curr_ctx_switches
                
                # Classify phase
                phase, details = self.classify_comm_phase(
                    pkg_power_W, dram_power_W, cpu_metrics, 
                    voluntary_ctx_rate, len(pids)
                )
                
                # Log data
                timestamp = time.time() - self.start_time
                row = {
                    'timestamp': f"{timestamp:.2f}",
                    'phase': details['phase'],
                    'confidence': f"{details['confidence']:.3f}",
                    'power_W': f"{details['power_W']:.1f}",
                    'dram_power_W': f"{details['dram_power_W']:.1f}",
                    'cpu_user_pct': f"{details['cpu_user_pct']:.1f}",
                    'cpu_system_pct': f"{details['cpu_system_pct']:.1f}",
                    'cpu_idle_pct': f"{details['cpu_idle_pct']:.1f}",
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'voluntary_ctx_switches_per_sec': f"{details['voluntary_ctx_switches_per_sec']:.0f}",
                    'active_mpi_ranks': details['active_mpi_ranks'],
                    'pkg_energy_J': f"{delta_pkg_energy:.4f}",
                    'dram_energy_J': f"{delta_dram_energy:.4f}",
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status
                if sample_count % 5 == 0:
                    print(f"[{timestamp:6.1f}s] {details['phase']:15} "
                          f"Conf:{details['confidence']:.2f} "
                          f"Power:{details['power_W']:5.0f}W "
                          f"Idle:{details['cpu_idle_pct']:4.0f}% "
                          f"Ctx:{details['voluntary_ctx_switches_per_sec']:4.0f}/s "
                          f"Ranks:{details['active_mpi_ranks']}")
                
        except KeyboardInterrupt:
            print("\n[INFO] Monitoring interrupted by user")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.csv_file:
            self.csv_file.close()
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")

def main():
    """Entry point"""
    
    # Validate environment
    if not os.path.exists(RAPL_PATH):
        print(f"[ERROR] RAPL not accessible at {RAPL_PATH}")
        print("[ERROR] Cannot monitor without energy data")
        return 1
    
    print("="*60)
    print("MiniMD Communication Phase Monitor - Production Version")
    print("="*60)
    print("Detection method: /proc/stat + RAPL + context switches")
    print("No perf permissions required")
    print(f"Thresholds: Power<{POWER_COMM_LOW}W, Idle>{CPU_IDLE_THRESHOLD}%, CtxSw>{VOLUNTARY_CTX_HIGH}/s")
    print("="*60)
    
    monitor = CommPhaseMonitor()
    
    try:
        monitor.run_monitoring()
    except Exception as e:
        print(f"[FATAL] {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n[INFO] Analysis complete. Check CSV for detailed metrics.")
    print(f"[INFO] Expected: ~97% COMMUNICATION phase (miniMD is MPI-dominated)")
    
    return 0

if __name__ == "__main__":
    exit(main())
