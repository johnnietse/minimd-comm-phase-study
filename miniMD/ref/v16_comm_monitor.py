#!/usr/bin/env python3
"""
INTELLIGENT Communication Phase Monitor for miniMD - Version 12.0
Improved with realistic classification, proper rank detection, and better metrics.
Key improvements:
1. Proper MPI rank detection (excludes parent process)
2. Realistic phase classification based on actual miniMD timings
3. Simplified metrics that actually work
4. Ground truth validation with miniMD output
5. Fixed CPU normalization issues
"""

import subprocess
import time
import csv
import os
import sys
import math
import statistics
import json
import argparse
import re
from pathlib import Path
from collections import defaultdict, deque, Counter
from datetime import datetime

# ============ CONFIGURATION ============
# Default command - can be overridden
DEFAULT_CMD = ["mpirun", "--oversubscribe", "-np", "32", 
               "./miniMD_openmpi", "-i", "in.lj.miniMD"]

LOG_FILE = f"comm_phase_monitoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
SUMMARY_FILE = f"comm_phase_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
SCALING_FILE = f"scaling_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
SAMPLE_INTERVAL = 0.2

# Empirical scaling data from actual miniMD runs
EMPIRICAL_SCALING_DATA = {
    1: {"comm_pct": 5.0, "compute_pct": 85.0, "force_pct": 85.0, "neigh_pct": 10.0},
    2: {"comm_pct": 23.7, "compute_pct": 62.5, "force_pct": 62.5, "neigh_pct": 12.6},
    4: {"comm_pct": 40.2, "compute_pct": 47.5, "force_pct": 47.5, "neigh_pct": 11.7},
    8: {"comm_pct": 75.5, "compute_pct": 14.9, "force_pct": 14.9, "neigh_pct": 9.3},
    16: {"comm_pct": 87.5, "compute_pct": 4.6, "force_pct": 4.6, "neigh_pct": 7.7},
    32: {"comm_pct": 96.9, "compute_pct": 1.9, "force_pct": 1.9, "neigh_pct": 0.3},
    64: {"comm_pct": 97.8, "compute_pct": 0.8, "force_pct": 0.8, "neigh_pct": 0.2}
}

def find_rapl_paths():
    """Dynamically find RAPL paths"""
    rapl_path = None
    dram_path = None
    
    # Common RAPL paths
    possible_paths = [
        "/sys/class/powercap/intel-rapl:0/energy_uj",
        "/sys/class/powercap/intel-rapl/energy_uj",
        "/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            rapl_path = path
            print(f"[INFO] Found RAPL at: {path}")
            break
    
    # Look for DRAM path
    if rapl_path:
        base_dir = os.path.dirname(rapl_path)
        try:
            for subdir in os.listdir(base_dir):
                if "dram" in subdir.lower() or ":0:0" in subdir:
                    dram_candidate = os.path.join(base_dir, subdir, "energy_uj")
                    if os.path.exists(dram_candidate):
                        dram_path = dram_candidate
                        print(f"[INFO] Found DRAM RAPL at: {dram_path}")
                        break
        except FileNotFoundError:
            pass
    
    return rapl_path, dram_path

# Dynamically find RAPL paths
RAPL_PATH, RAPL_DRAM_PATH = find_rapl_paths()

def parse_miniMD_output(output):
    """Parse miniMD output to extract actual timings"""
    result = {
        "mpi_procs": None,
        "t_total": None,
        "t_comm": None,
        "t_force": None,
        "t_neigh": None,
        "performance": None,
        "valid": False
    }
    
    # Extract MPI processes
    mpi_match = re.search(r"MPI processes:\s+(\d+)", output)
    if mpi_match:
        result["mpi_procs"] = int(mpi_match.group(1))
    
    # Extract performance summary (the last line with numbers)
    lines = output.split('\n')
    for line in lines:
        if 'PERF_SUMMARY' in line:
            parts = line.split()
            if len(parts) >= 12:
                try:
                    result["mpi_procs"] = int(parts[0])
                    result["t_total"] = float(parts[4])
                    result["t_force"] = float(parts[5])
                    result["t_neigh"] = float(parts[6])
                    result["t_comm"] = float(parts[7])
                    result["performance"] = float(parts[9])
                    result["valid"] = True
                except (ValueError, IndexError):
                    pass
    
    # Calculate percentages
    if result["t_total"] and result["t_total"] > 0:
        if result["t_comm"]:
            result["comm_pct"] = (result["t_comm"] / result["t_total"]) * 100
        if result["t_force"]:
            result["compute_pct"] = ((result["t_force"] + result.get("t_neigh", 0)) / result["t_total"]) * 100
    
    return result

class IntelligentCommPhaseMonitor:
    def __init__(self, expected_ranks=None):
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.prev_dram_energy = None
        self.prev_proc_stats = {}
        self.prev_ctx_switches = (0, 0)
        
        self.has_dram_rapl = RAPL_DRAM_PATH is not None and os.path.exists(RAPL_DRAM_PATH)
        
        # Phase tracking
        self.current_phase = "INIT"
        self.phase_start_time = None
        self.phase_stats = defaultdict(lambda: {"time": 0.0, "samples": 0, "transitions": 0})
        self.phase_sequence = []
        
        # Expected behavior
        self.expected_ranks = expected_ranks
        self.actual_detected_ranks = None
        self.miniMD_timings = None
        
        # Use empirical data for calibration
        if expected_ranks in EMPIRICAL_SCALING_DATA:
            self.expected_comm_pct = EMPIRICAL_SCALING_DATA[expected_ranks]["comm_pct"]
            self.expected_compute_pct = EMPIRICAL_SCALING_DATA[expected_ranks]["compute_pct"]
            print(f"[INFO] Using empirical data: {expected_ranks} ranks => "
                  f"{self.expected_comm_pct:.1f}% comm, {self.expected_compute_pct:.1f}% compute")
        else:
            self.estimate_expected_behavior()
        
        # History for smoothing
        self.power_history = deque(maxlen=5)
        self.cpu_history = deque(maxlen=5)
        self.ctx_history = deque(maxlen=5)
        
        print(f"[INFO] Expected ranks: {self.expected_ranks}")
    
    def estimate_expected_behavior(self):
        """Estimate expected behavior based on empirical scaling data"""
        if not self.expected_ranks:
            self.expected_ranks = 32
        
        available_ranks = sorted(EMPIRICAL_SCALING_DATA.keys())
        nearest = min(available_ranks, key=lambda x: abs(x - self.expected_ranks))
        
        if nearest == self.expected_ranks:
            self.expected_comm_pct = EMPIRICAL_SCALING_DATA[nearest]["comm_pct"]
            self.expected_compute_pct = EMPIRICAL_SCALING_DATA[nearest]["compute_pct"]
        else:
            # Linear interpolation
            lower = None
            upper = None
            for i in range(len(available_ranks) - 1):
                if available_ranks[i] <= self.expected_ranks <= available_ranks[i + 1]:
                    lower = available_ranks[i]
                    upper = available_ranks[i + 1]
                    break
            
            if lower and upper:
                lower_comm = EMPIRICAL_SCALING_DATA[lower]["comm_pct"]
                upper_comm = EMPIRICAL_SCALING_DATA[upper]["comm_pct"]
                
                t = (self.expected_ranks - lower) / (upper - lower)
                self.expected_comm_pct = lower_comm + t * (upper_comm - lower_comm)
                self.expected_compute_pct = 100 - self.expected_comm_pct
            else:
                self.expected_comm_pct = 50.0
                self.expected_compute_pct = 50.0
        
        print(f"[INFO] Estimated for {self.expected_ranks} ranks: "
              f"{self.expected_comm_pct:.1f}% comm, {self.expected_compute_pct:.1f}% compute")
    
    def get_cpu_cores(self):
        """Get total number of CPU cores"""
        try:
            with open("/proc/cpuinfo", "r") as f:
                return f.read().count("processor\t:")
        except:
            return 64  # Reasonable default
    
    def detect_mpi_ranks(self):
        """Properly detect MPI ranks by counting miniMD worker processes"""
        try:
            # First try to get all processes
            result = subprocess.run(["pgrep", "-f", "miniMD"], 
                                  capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                all_pids = [int(pid) for pid in result.stdout.strip().split()]
                
                # Now filter out non-worker processes
                worker_pids = []
                for pid in all_pids:
                    try:
                        with open(f"/proc/{pid}/cmdline", "r") as f:
                            cmdline = f.read().lower()
                            # Look for actual miniMD worker processes
                            if "minimd" in cmdline and "mpirun" not in cmdline:
                                worker_pids.append(pid)
                    except:
                        continue
                
                if worker_pids:
                    self.actual_detected_ranks = len(worker_pids)
                    return worker_pids
                
                # Fallback: if we can't filter, assume all are workers
                self.actual_detected_ranks = len(all_pids)
                return all_pids
        except Exception as e:
            print(f"[WARN] PID detection failed: {e}")
        
        return []
    
    def safe_delta(self, current, previous, counter_name=""):
        """Safe delta calculation with counter wrap-around handling"""
        if current >= previous:
            return current - previous
        
        # Different max values for different counters
        if "energy_uj" in counter_name.lower():
            max_value = 2**32  # RAPL counters
        elif "ctx" in counter_name.lower():
            max_value = 2**64
        else:
            max_value = 2**32
        
        delta = (max_value - previous) + current
        return delta
    
    def read_rapl_energy(self, path):
        """Read RAPL energy counter (microjoules)"""
        if not path or not os.path.exists(path):
            return 0
        
        try:
            with open(path, "r") as f:
                return int(f.read().strip())
        except:
            return 0
    
    def read_proc_stat(self):
        """Read /proc/stat for system-wide CPU metrics"""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
                parts = line.split()
                if parts[0] == 'cpu':
                    return [int(x) for x in parts[1:9]]
        except:
            return [0] * 8
    
    def calculate_cpu_usage(self, prev_stats, curr_stats):
        """Calculate CPU usage percentages from /proc/stat"""
        if not prev_stats or len(prev_stats) < 8 or len(curr_stats) < 8:
            return 0, 0, 100, 0
        
        total_delta = 0
        deltas = []
        for i in range(8):
            delta = self.safe_delta(curr_stats[i], prev_stats[i], f"cpu_stat_{i}")
            deltas.append(delta)
            total_delta += delta
        
        if total_delta == 0:
            return 0, 0, 100, 0
        
        user_pct = 100.0 * deltas[0] / total_delta
        system_pct = 100.0 * deltas[2] / total_delta
        idle_pct = 100.0 * deltas[3] / total_delta
        iowait_pct = 100.0 * deltas[4] / total_delta
        
        return user_pct, system_pct, idle_pct, iowait_pct
    
    def get_proc_context_switches(self, pids):
        """Get context switches for processes"""
        total_voluntary = 0
        total_nonvoluntary = 0
        
        for pid in pids:
            try:
                with open(f"/proc/{pid}/status", "r") as f:
                    for line in f:
                        if line.startswith("voluntary_ctxt_switches"):
                            total_voluntary += int(line.split()[1])
                        elif line.startswith("nonvoluntary_ctxt_switches"):
                            total_nonvoluntary += int(line.split()[1])
            except:
                continue
        
        return total_voluntary, total_nonvoluntary
    
    def classify_phase_simple(self, pkg_power_W, cpu_metrics, ctx_rate, timestamp):
        """
        Simple but effective phase classification based on actual patterns observed
        """
        user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
        active_cpu = 100 - idle_pct
        
        # Get actual rank count for normalization
        actual_ranks = self.actual_detected_ranks or self.expected_ranks
        if actual_ranks <= 0:
            actual_ranks = 1
        
        # Normalize power per rank
        power_per_rank = pkg_power_W / actual_ranks
        
        # Store history for smoothing
        self.power_history.append(power_per_rank)
        self.cpu_history.append(active_cpu)
        self.ctx_history.append(ctx_rate)
        
        # Calculate moving averages
        avg_power = statistics.mean(self.power_history) if len(self.power_history) > 0 else power_per_rank
        avg_cpu = statistics.mean(self.cpu_history) if len(self.cpu_history) > 0 else active_cpu
        avg_ctx = statistics.mean(self.ctx_history) if len(self.ctx_history) > 0 else ctx_rate
        
        # Determine phase based on clear patterns
        phase = "MIXED"
        confidence = 0.5
        reasons = []
        
        # PATTERN 1: Very low CPU usage (< 5%) = IDLE or FINISHING
        if idle_pct > 95:
            phase = "IDLE"
            confidence = 0.8
            reasons.append(f"idle={idle_pct:.1f}%")
        
        # PATTERN 2: Moderate CPU usage with context switches = COMMUNICATION
        elif 5 < active_cpu < 30 and ctx_rate > 100:
            phase = "COMMUNICATION"
            confidence = 0.7
            reasons.append(f"cpu={active_cpu:.1f}%, ctx={ctx_rate:.0f}/s")
        
        # PATTERN 3: High CPU usage (> 30%) = COMPUTE
        elif active_cpu > 30:
            phase = "COMPUTE"
            confidence = 0.7
            reasons.append(f"cpu={active_cpu:.1f}%")
        
        # PATTERN 4: Based on expected behavior for this rank count
        elif actual_ranks in EMPIRICAL_SCALING_DATA:
            expected_comm = EMPIRICAL_SCALING_DATA[actual_ranks]["comm_pct"]
            if expected_comm > 70:  # Communication-bound workload
                phase = "COMMUNICATION"
                confidence = 0.6
                reasons.append(f"exp_comm={expected_comm:.1f}%")
            elif expected_comm < 30:  # Compute-bound workload
                phase = "COMPUTE"
                confidence = 0.6
                reasons.append(f"exp_comm={expected_comm:.1f}%")
        
        # Stabilize phase
        phase = self.stabilize_phase(phase)
        
        # Build results
        results = {
            'phase': phase,
            'confidence': confidence,
            'total_power_W': pkg_power_W,
            'power_per_rank_W': power_per_rank,
            'cpu_user_pct': user_pct,
            'cpu_system_pct': system_pct,
            'cpu_idle_pct': idle_pct,
            'cpu_iowait_pct': iowait_pct,
            'active_cpu_pct': active_cpu,
            'ctx_rate': ctx_rate,
            'actual_ranks': actual_ranks,
            'reasons': ', '.join(reasons) if reasons else 'balanced'
        }
        
        return phase, results
    
    def stabilize_phase(self, proposed_phase):
        """Simple phase stabilization"""
        self.phase_sequence.append(proposed_phase)
        if len(self.phase_sequence) > 3:
            self.phase_sequence.pop(0)
        
        if len(self.phase_sequence) >= 3:
            counts = Counter(self.phase_sequence)
            most_common = counts.most_common(1)[0]
            if most_common[1] >= 2 and most_common[0] != proposed_phase:
                return most_common[0]
        
        return proposed_phase
    
    def track_phase_duration(self, phase, timestamp):
        """Track phase duration"""
        if self.current_phase is None:
            self.current_phase = phase
            self.phase_start_time = timestamp
        elif self.current_phase != phase:
            # Phase changed, record duration
            if self.phase_start_time is not None:
                duration = timestamp - self.phase_start_time
                if duration > 0:
                    self.phase_stats[self.current_phase]["time"] += duration
                    self.phase_stats[self.current_phase]["samples"] += 1
                    self.phase_stats[self.current_phase]["transitions"] += 1
            
            # Start new phase
            self.current_phase = phase
            self.phase_start_time = timestamp
    
    def setup_csv(self):
        """Initialize CSV logging"""
        try:
            self.csv_file = open(LOG_FILE, "w", newline='')
            fieldnames = [
                'timestamp',
                'phase',
                'confidence',
                'total_power_W',
                'power_per_rank_W',
                'cpu_user_pct',
                'cpu_system_pct', 
                'cpu_idle_pct',
                'cpu_iowait_pct',
                'active_cpu_pct',
                'ctx_rate',
                'actual_ranks',
                'reasons'
            ]
            self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
            self.writer.writeheader()
            print(f"[INFO] Logging to {LOG_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to create CSV file: {e}")
            raise
    
    def run_monitoring(self, miniMD_process=None):
        """Main monitoring loop"""
        print(f"[INFO] Starting communication phase monitoring")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        
        self.setup_csv()
        
        # Initialize counters
        self.prev_energy = self.read_rapl_energy(RAPL_PATH)
        if self.has_dram_rapl:
            self.prev_dram_energy = self.read_rapl_energy(RAPL_DRAM_PATH)
        
        self.prev_proc_stats = self.read_proc_stat()
        self.prev_ctx_switches = (0, 0)
        
        self.start_time = time.time()
        sample_count = 0
        
        try:
            while True:
                # Check if miniMD still running
                if miniMD_process and miniMD_process.poll() is not None:
                    print("[INFO] miniMD process completed")
                    break
                
                # Detect MPI ranks
                pids = self.detect_mpi_ranks()
                if not pids and miniMD_process is None:
                    print("[INFO] No miniMD processes detected")
                    break
                
                sample_count += 1
                current_time = time.time() - self.start_time
                
                # Sleep for sampling interval
                time.sleep(SAMPLE_INTERVAL)
                
                # Read current energy
                energy_now = self.read_rapl_energy(RAPL_PATH)
                delta_pkg_energy = self.safe_delta(energy_now, self.prev_energy, "pkg_energy") / 1e6
                self.prev_energy = energy_now
                
                # Calculate package power
                pkg_power_W = delta_pkg_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Read CPU utilization
                curr_proc_stats = self.read_proc_stat()
                cpu_metrics = self.calculate_cpu_usage(self.prev_proc_stats, curr_proc_stats)
                self.prev_proc_stats = curr_proc_stats
                
                # Read context switches
                curr_ctx_switches = self.get_proc_context_switches(pids)
                ctx_rate = self.safe_delta(
                    curr_ctx_switches[0], self.prev_ctx_switches[0],
                    SAMPLE_INTERVAL, "voluntary_ctx"
                )
                self.prev_ctx_switches = curr_ctx_switches
                
                # Classify phase
                phase, details = self.classify_phase_simple(
                    pkg_power_W, cpu_metrics, ctx_rate, current_time
                )
                
                # Track phase duration
                self.track_phase_duration(phase, current_time)
                
                # Log data
                row = {
                    'timestamp': f"{current_time:.2f}",
                    'phase': details['phase'],
                    'confidence': f"{details['confidence']:.3f}",
                    'total_power_W': f"{details['total_power_W']:.1f}",
                    'power_per_rank_W': f"{details['power_per_rank_W']:.1f}",
                    'cpu_user_pct': f"{details['cpu_user_pct']:.1f}",
                    'cpu_system_pct': f"{details['cpu_system_pct']:.1f}",
                    'cpu_idle_pct': f"{details['cpu_idle_pct']:.1f}",
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'active_cpu_pct': f"{details['active_cpu_pct']:.1f}",
                    'ctx_rate': f"{details['ctx_rate']:.0f}",
                    'actual_ranks': details['actual_ranks'],
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status
                if sample_count % 5 == 0:
                    phase_char = details['phase'][0] if details['phase'] else '?'
                    actual_ranks = details['actual_ranks']
                    print(f"[{current_time:6.1f}s] {phase_char} "
                          f"R:{actual_ranks:2d} "
                          f"Pwr/R:{details['power_per_rank_W']:4.1f}W "
                          f"CPU:{details['active_cpu_pct']:3.0f}% "
                          f"C:{details['confidence']:.2f}")
                
        except KeyboardInterrupt:
            print("\n[INFO] Monitoring interrupted by user")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if miniMD_process and miniMD_process.poll() is None:
                print("[INFO] Terminating miniMD process...")
                miniMD_process.terminate()
                try:
                    miniMD_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    miniMD_process.kill()
            self.cleanup()
    
    def generate_report(self, total_time, miniMD_output=None):
        """Generate analysis report"""
        try:
            # Parse miniMD output if available
            if miniMD_output:
                self.miniMD_timings = parse_miniMD_output(miniMD_output)
            
            with open(SUMMARY_FILE, "w") as f:
                f.write("="*80 + "\n")
                f.write("MiniMD Communication Phase Analysis - Version 12.0\n")
                f.write("="*80 + "\n\n")
                
                # Run information
                f.write("RUN INFORMATION:\n")
                f.write("-"*50 + "\n")
                actual_ranks = self.actual_detected_ranks or self.expected_ranks
                f.write(f"Expected MPI ranks: {self.expected_ranks}\n")
                f.write(f"Detected MPI ranks: {actual_ranks}\n")
                f.write(f"Total monitoring time: {total_time:.2f}s\n")
                f.write("\n")
                
                # Empirical expectations
                f.write("EMPIRICAL EXPECTATIONS:\n")
                f.write("-"*50 + "\n")
                if actual_ranks in EMPIRICAL_SCALING_DATA:
                    f.write(f"Expected communication: {EMPIRICAL_SCALING_DATA[actual_ranks]['comm_pct']:.1f}%\n")
                    f.write(f"Expected compute: {EMPIRICAL_SCALING_DATA[actual_ranks]['compute_pct']:.1f}%\n")
                f.write("\n")
                
                # Phase analysis
                f.write("PHASE ANALYSIS:\n")
                f.write("-"*50 + "\n")
                
                total_tracked = sum(stats["time"] for stats in self.phase_stats.values())
                
                for phase in ["COMPUTE", "COMMUNICATION", "MIXED", "IDLE"]:
                    if phase in self.phase_stats:
                        stats = self.phase_stats[phase]
                        duration = stats["time"]
                        percentage = (duration / total_time) * 100 if total_time > 0 else 0
                        f.write(f"{phase:20s}: {duration:7.2f}s ({percentage:6.2f}%)\n")
                
                f.write("\n")
                
                # MiniMD ground truth if available
                if self.miniMD_timings and self.miniMD_timings["valid"]:
                    f.write("MINIMD GROUND TRUTH:\n")
                    f.write("-"*50 + "\n")
                    timing = self.miniMD_timings
                    f.write(f"MPI processes: {timing['mpi_procs']}\n")
                    f.write(f"Total time: {timing['t_total']:.3f}s\n")
                    f.write(f"Communication time: {timing['t_comm']:.3f}s ({timing.get('comm_pct', 0):.1f}%)\n")
                    f.write(f"Compute time: {timing['t_force']:.3f}s ({timing.get('compute_pct', 0):.1f}%)\n")
                    f.write(f"Neighbor time: {timing['t_neigh']:.3f}s\n")
                    f.write("\n")
                    
                    # Compare with our detection
                    if "COMMUNICATION" in self.phase_stats:
                        our_comm = self.phase_stats["COMMUNICATION"]["time"]
                        our_comm_pct = (our_comm / total_time * 100) if total_time > 0 else 0
                        miniMD_comm_pct = timing.get('comm_pct', 0)
                        diff = our_comm_pct - miniMD_comm_pct
                        
                        f.write("VALIDATION:\n")
                        f.write("-"*50 + "\n")
                        f.write(f"Our detection: {our_comm_pct:.1f}% communication\n")
                        f.write(f"MiniMD actual: {miniMD_comm_pct:.1f}% communication\n")
                        f.write(f"Difference: {diff:+.1f}%\n")
                        
                        if abs(diff) < 10:
                            f.write("✓ Good match!\n")
                        elif abs(diff) < 20:
                            f.write("~ Reasonable match\n")
                        else:
                            f.write("⚠ Significant deviation\n")
                
                # Recommendations
                f.write("\n" + "="*80 + "\n")
                f.write("RECOMMENDATIONS:\n")
                f.write("="*80 + "\n\n")
                
                if actual_ranks <= 4:
                    f.write("WORKLOAD CHARACTERIZATION: COMPUTE-BOUND\n")
                    f.write("- Focus on computational efficiency\n")
                    f.write("- Communication overhead is relatively low (20-40%)\n")
                    f.write("- Consider increasing problem size per rank\n")
                elif actual_ranks <= 16:
                    f.write("WORKLOAD CHARACTERIZATION: BALANCED\n")
                    f.write("- Good balance between computation and communication\n")
                    f.write("- Both aspects important for performance\n")
                    f.write("- Consider optimizing both computation and communication\n")
                else:
                    f.write("WORKLOAD CHARACTERIZATION: COMMUNICATION-BOUND\n")
                    f.write("- Communication overhead dominates (>75%)\n")
                    f.write("- Problem size per rank is small\n")
                    f.write("- Focus on communication optimization (MPI tuning, network)\n")
            
            print(f"[INFO] Report saved to {SUMMARY_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to generate report: {e}")
    
    def generate_scaling_analysis(self):
        """Generate scaling analysis"""
        scaling_data = {
            "empirical_data": EMPIRICAL_SCALING_DATA,
            "current_run": {
                "expected_ranks": self.expected_ranks,
                "detected_ranks": self.actual_detected_ranks,
            },
            "phase_stats": dict(self.phase_stats)
        }
        
        # Save to file
        try:
            with open(SCALING_FILE, 'w') as f:
                json.dump(scaling_data, f, indent=2)
            print(f"[INFO] Scaling analysis saved to {SCALING_FILE}")
        except Exception as e:
            print(f"[WARN] Failed to save scaling analysis: {e}")
        
        return scaling_data
    
    def cleanup(self):
        """Cleanup and generate reports"""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        # Record final phase duration
        if self.current_phase and self.phase_start_time:
            final_duration = total_time - self.phase_start_time
            if final_duration > 0:
                self.phase_stats[self.current_phase]["time"] += final_duration
                self.phase_stats[self.current_phase]["samples"] += 1
        
        if self.csv_file:
            try:
                self.csv_file.close()
            except:
                pass
        
        # Generate reports
        self.generate_scaling_analysis()
        
        # Note: miniMD output will be passed separately
        self.generate_report(total_time)
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
        print(f"[INFO] Scaling analysis saved to {SCALING_FILE}")
        
        # Print quick summary
        print("\n" + "="*60)
        print("QUICK SUMMARY:")
        print("="*60)
        for phase in ["COMPUTE", "COMMUNICATION", "MIXED", "IDLE"]:
            if phase in self.phase_stats:
                stats = self.phase_stats[phase]
                duration = stats["time"]
                percentage = (duration / total_time * 100) if total_time > 0 else 0
                print(f"{phase:20s}: {duration:7.2f}s ({percentage:6.2f}%)")

def main():
    """Entry point"""
    parser = argparse.ArgumentParser(description='MiniMD Communication Phase Monitor')
    parser.add_argument('-n', '--ranks', type=int, default=32,
                       help='Number of MPI ranks')
    parser.add_argument('-c', '--command', type=str,
                       help='Full command to run miniMD')
    parser.add_argument('-i', '--interval', type=float, default=0.2,
                       help='Sampling interval in seconds')
    parser.add_argument('--no-run', action='store_true',
                       help='Monitor existing miniMD process')
    
    args = parser.parse_args()
    
    print("="*80)
    print("MiniMD Communication Phase Monitor - Version 12.0")
    print("="*80)
    print("Key improvements:")
    print("  ✓ Proper MPI rank detection")
    print("  ✓ Simple but effective classification")
    print("  ✓ Ground truth validation with miniMD output")
    print("  ✓ Realistic CPU utilization metrics")
    print("="*80)
    print(f"Configuration:")
    print(f"  MPI ranks: {args.ranks}")
    print(f"  Sample interval: {args.interval}s")
    print("="*80)
    
    # Update configuration
    global SAMPLE_INTERVAL
    SAMPLE_INTERVAL = args.interval
    
    # Build command
    if args.command:
        import shlex
        cmd = shlex.split(args.command)
    else:
        cmd = DEFAULT_CMD.copy()
        # Update rank count in default command
        for i, arg in enumerate(cmd):
            if arg in ['-np', '-n'] and i + 1 < len(cmd):
                cmd[i + 1] = str(args.ranks)
                break
    
    monitor = IntelligentCommPhaseMonitor(expected_ranks=args.ranks)
    
    try:
        if args.no_run:
            print("[INFO] Monitoring existing miniMD process")
            monitor.run_monitoring()
        else:
            print(f"[INFO] Launching miniMD: {' '.join(cmd)}")
            try:
                miniMD_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                
                # Wait a moment for miniMD to start
                time.sleep(2)
                
                # Start monitoring
                monitor.run_monitoring(miniMD_process)
                
                # Capture miniMD output
                stdout, stderr = miniMD_process.communicate(timeout=10)
                
                # Store output for report
                miniMD_output = stdout
                
                print("\n[INFO] MiniMD output:")
                print(stdout[:2000])
                
                # Regenerate report with miniMD timings
                if monitor.start_time:
                    total_time = time.time() - monitor.start_time
                    monitor.generate_report(total_time, miniMD_output)
                    
            except FileNotFoundError as e:
                print(f"[ERROR] Failed to start miniMD: {e}")
                return 1
            except Exception as e:
                print(f"[ERROR] Error running miniMD: {e}")
                return 1
    
    except KeyboardInterrupt:
        print("\n[INFO] Program interrupted by user")
    except Exception as e:
        print(f"[FATAL] {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
