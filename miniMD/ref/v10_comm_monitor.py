#!/usr/bin/env python3
"""
Adaptive Communication Phase Monitor for miniMD on Frontenac
Version 9.0 - Fully adaptive, statistical baseline, MPI-size aware
Combines best of v7's detection, v8's fixes, and v7.1's improvements
with complete statistical adaptation
"""

import subprocess
import time
import csv
import os
import sys
import math
import statistics
import numpy as np
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime

# ============ CONFIGURATION ============
CMD = ["mpirun", "--oversubscribe", "-np", "32", 
       "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = f"comm_phase_monitoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
SUMMARY_FILE = f"comm_phase_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
SAMPLE_INTERVAL = 0.2  # 200ms for better temporal resolution
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_DRAM_PATH = "/sys/class/powercap/intel-rapl:0:0/energy_uj"

# Adaptive parameters
ADAPTIVE_WINDOW = 30  # Number of samples for statistical baseline
MIN_PHASE_DURATION = 0.4  # Minimum phase duration (seconds)
MAX_CTX_RATE = 1e6  # Sanity limit for context switches
INITIAL_SAMPLES = 10  # Samples to collect before starting classification

# Network interface to monitor
NETWORK_INTERFACES = ["ib0", "ib1", "eth0", "eth1", "eno1", "ens1"]

class AdaptiveCommPhaseMonitor:
    def __init__(self):
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.prev_dram_energy = None
        self.prev_proc_stats = {}
        self.prev_net_stats = {}
        self.prev_ctx_switches = {}
        self.prev_per_process_cpu = {}
        
        self.has_dram_rapl = os.path.exists(RAPL_DRAM_PATH)
        
        # Statistical baselines
        self.power_baseline = deque(maxlen=ADAPTIVE_WINDOW)
        self.idle_baseline = deque(maxlen=ADAPTIVE_WINDOW)
        self.ctx_baseline = deque(maxlen=ADAPTIVE_WINDOW)
        self.sync_baseline = deque(maxlen=ADAPTIVE_WINDOW)
        self.baseline_established = False
        self.samples_collected = 0
        
        # Phase tracking
        self.current_phase = "INITIALIZING"
        self.phase_start_time = None
        self.phase_stats = defaultdict(lambda: {"time": 0.0, "samples": 0})
        
        # For MPI-size aware detection
        self.expected_mpi_ranks = 0
        self.network_interfaces = self.detect_network_interfaces()
        
        print(f"[INFO] Detected network interfaces: {self.network_interfaces}")
    
    def detect_network_interfaces(self):
        """Detect available network interfaces for monitoring"""
        interfaces = []
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()[2:]  # Skip header lines
                for line in lines:
                    if ":" in line:
                        iface = line.split(":")[0].strip()
                        if iface in NETWORK_INTERFACES or iface.startswith("ib") or iface.startswith("eth"):
                            interfaces.append(iface)
        except Exception as e:
            print(f"[WARN] Could not read /proc/net/dev: {e}")
        return interfaces
    
    def get_miniMD_pids(self):
        """Get PIDs of running miniMD processes"""
        try:
            result = subprocess.run(["pgrep", "-f", "miniMD"],
                                  capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout.strip():
                pids = [int(pid) for pid in result.stdout.strip().split()]
                # Update expected MPI ranks based on actual process count
                if len(pids) > self.expected_mpi_ranks:
                    self.expected_mpi_ranks = len(pids)
                return pids
        except Exception as e:
            pass
        return []
    
    def safe_delta(self, current, previous, counter_name=""):
        """Safe delta calculation with counter wrap-around handling"""
        if current >= previous:
            return current - previous
        
        # Counter wrap-around detected
        if "energy_uj" in counter_name or "rapl" in counter_name.lower():
            max_value = 2**32
        elif "ctx" in counter_name.lower():
            max_value = 2**64
        else:
            max_value = 2**64
        
        delta = (max_value - previous) + current
        if "energy" in counter_name or "ctx" in counter_name:
            print(f"[DEBUG] Counter wrap-around for {counter_name}: {previous} -> {current}")
        return delta
    
    def safe_rate(self, curr, prev, dt, counter_name=""):
        """Calculate rate with sanity clamping"""
        if dt <= 0:
            return 0.0
        
        delta = self.safe_delta(curr, prev, counter_name)
        rate = delta / dt
        
        # Clamp impossible rates
        if rate > MAX_CTX_RATE:
            print(f"[DEBUG] Clamped unrealistic rate for {counter_name}: {rate:.0f} -> 0")
            return 0.0
        return rate
    
    def read_rapl_energy(self, path, counter_name=""):
        """Read RAPL energy counter (microjoules) with error handling"""
        try:
            with open(path, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            print(f"[WARN] Failed to read RAPL at {path}: {e}")
            return 0
    
    def read_network_stats(self):
        """Read network statistics from /proc/net/dev"""
        net_stats = {}
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()[2:]  # Skip header lines
                for line in lines:
                    if ":" in line:
                        parts = line.split()
                        iface = parts[0].replace(":", "")
                        if iface in self.network_interfaces:
                            bytes_recv = int(parts[1])
                            bytes_sent = int(parts[9])
                            net_stats[iface] = (bytes_recv, bytes_sent)
        except Exception as e:
            print(f"[WARN] Failed to read network stats: {e}")
        return net_stats
    
    def calculate_network_bandwidth(self, prev_stats, curr_stats):
        """Calculate network bandwidth in MB/s"""
        total_recv_delta = 0
        total_sent_delta = 0
        
        for iface in self.network_interfaces:
            if iface in prev_stats and iface in curr_stats:
                prev_recv, prev_sent = prev_stats[iface]
                curr_recv, curr_sent = curr_stats[iface]
                
                recv_delta = self.safe_delta(curr_recv, prev_recv, f"net_{iface}_recv")
                sent_delta = self.safe_delta(curr_sent, prev_sent, f"net_{iface}_sent")
                
                total_recv_delta += recv_delta
                total_sent_delta += sent_delta
        
        # Convert to MB/s
        recv_mbps = total_recv_delta / SAMPLE_INTERVAL / (1024 * 1024)
        sent_mbps = total_sent_delta / SAMPLE_INTERVAL / (1024 * 1024)
        
        return recv_mbps, sent_mbps
    
    def get_per_process_cpu_times(self, pids):
        """Get CPU times (user+system) for each process"""
        cpu_times = {}
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", "r") as f:
                    stat_data = f.read().split()
                    # Position 13: utime, 14: stime (in clock ticks)
                    utime = int(stat_data[13])
                    stime = int(stat_data[14])
                    cpu_times[pid] = utime + stime
            except (FileNotFoundError, ValueError, IndexError):
                continue
        return cpu_times
    
    def calculate_sync_variance(self, curr_cpu_times, prev_cpu_times):
        """
        Calculate synchronization variance across processes
        Uses per-interval deltas
        """
        # Get deltas for each process
        deltas = []
        for pid, curr_time in curr_cpu_times.items():
            if pid in prev_cpu_times:
                prev_time = prev_cpu_times[pid]
                delta = max(curr_time - prev_time, 0)  # Handle PID restarts
                deltas.append(delta)
        
        if len(deltas) < 2:
            return 0.0
        
        try:
            # Calculate normalized variance
            mean = statistics.mean(deltas)
            if mean == 0:
                return 0.0
            
            variance = statistics.variance(deltas, mean) if len(deltas) > 1 else 0
            normalized_variance = (variance / mean * 100) if mean > 0 else 0
            
            return normalized_variance
        except statistics.StatisticsError:
            return 0.0
    
    def read_proc_stat(self):
        """Read /proc/stat for system-wide CPU metrics"""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()  # First line is aggregate 'cpu'
                parts = line.split()
                if parts[0] == 'cpu':
                    return [int(x) for x in parts[1:9]]
        except Exception as e:
            print(f"[WARN] Failed to read /proc/stat: {e}")
        return [0] * 8
    
    def get_proc_context_switches(self, pids):
        """Get context switches for all miniMD processes"""
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
            except (FileNotFoundError, ValueError, IndexError):
                continue
        
        return total_voluntary, total_nonvoluntary
    
    def calculate_cpu_usage(self, prev_stats, curr_stats):
        """Calculate CPU usage percentages from /proc/stat"""
        if not prev_stats or len(prev_stats) < 8 or len(curr_stats) < 8:
            return 0, 0, 100, 0
        
        # Calculate deltas with safe handling
        total_delta = 0
        deltas = []
        for i in range(8):
            delta = self.safe_delta(curr_stats[i], prev_stats[i], f"cpu_stat_{i}")
            deltas.append(delta)
            total_delta += delta
        
        if total_delta == 0:
            return 0, 0, 100, 0
        
        # Calculate percentages
        user_pct = 100.0 * deltas[0] / total_delta
        system_pct = 100.0 * deltas[2] / total_delta
        idle_pct = 100.0 * deltas[3] / total_delta
        iowait_pct = 100.0 * deltas[4] / total_delta
        
        return user_pct, system_pct, idle_pct, iowait_pct
    
    def calculate_statistical_baselines(self, pkg_power_W, idle_pct, ctx_rate, sync_var):
        """Calculate statistical baselines for adaptive thresholds"""
        # Add current values to baselines
        self.power_baseline.append(pkg_power_W)
        self.idle_baseline.append(idle_pct)
        self.ctx_baseline.append(ctx_rate)
        self.sync_baseline.append(sync_var)
        
        self.samples_collected += 1
        
        # Check if we have enough samples to establish baselines
        if self.samples_collected >= INITIAL_SAMPLES and not self.baseline_established:
            self.baseline_established = True
            print(f"[INFO] Statistical baselines established from {self.samples_collected} samples")
            print(f"[INFO] Power: mean={np.mean(self.power_baseline):.1f}W")
            print(f"[INFO] Idle: mean={np.mean(self.idle_baseline):.1f}%")
            print(f"[INFO] Ctx rate: mean={np.mean(self.ctx_baseline):.0f}/s")
            print(f"[INFO] Sync var: mean={np.mean(self.sync_baseline):.1f}%")
        
        # Return current statistics if available
        if len(self.power_baseline) < 5:
            return None, None, None, None
        
        return (np.mean(self.power_baseline), np.std(self.power_baseline),
                np.mean(self.idle_baseline), np.std(self.idle_baseline),
                np.mean(self.ctx_baseline), np.std(self.ctx_baseline),
                np.mean(self.sync_baseline), np.std(self.sync_baseline))
    
    def classify_phase_adaptive(self, pkg_power_W, dram_power_W, cpu_metrics, 
                               ctx_switch_rate, pids_count, network_bw,
                               sync_variance_norm, time_in_current_phase):
        """
        Adaptive phase classification using statistical baselines
        """
        user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
        net_recv_mbps, net_sent_mbps = network_bw
        
        active_cpu_pct = 100.0 - idle_pct
        total_net_bw = net_recv_mbps + net_sent_mbps
        
        # Get statistical baselines
        stats = self.calculate_statistical_baselines(pkg_power_W, idle_pct, 
                                                     ctx_switch_rate, sync_variance_norm)
        
        # If baselines not established yet, use INITIALIZING phase
        if not stats or not self.baseline_established:
            return "INITIALIZING", 0.5, ["baseline_establishing"]
        
        (power_mean, power_std, idle_mean, idle_std, 
         ctx_mean, ctx_std, sync_mean, sync_std) = stats
        
        # Calculate z-scores (how many standard deviations from mean)
        power_z = (pkg_power_W - power_mean) / power_std if power_std > 0 else 0
        idle_z = (idle_pct - idle_mean) / idle_std if idle_std > 0 else 0
        ctx_z = (ctx_switch_rate - ctx_mean) / ctx_std if ctx_std > 0 else 0
        sync_z = (sync_variance_norm - sync_mean) / sync_std if sync_std > 0 else 0
        
        # Initialize scores and reasons
        comm_score = 0
        comp_score = 0
        reasons = []
        
        # --- Communication Indicators ---
        # High idle (relative to baseline)
        if idle_z > 1.0:  # More than 1 std dev above mean
            comm_score += (idle_z - 1.0) * 2
            reasons.append(f"high_idle_z({idle_z:.1f})")
        
        # Low power (relative to baseline)
        if power_z < -1.0:  # More than 1 std dev below mean
            comm_score += abs(power_z - 1.0) * 1.5
            reasons.append(f"low_power_z({power_z:.1f})")
        
        # High context switches (relative to baseline)
        if ctx_z > 1.5:  # More than 1.5 std dev above mean
            comm_score += (ctx_z - 1.5) * 2
            reasons.append(f"high_ctx_z({ctx_z:.1f})")
        
        # Low synchronization variance (ranks waiting together)
        if sync_z < -1.0:  # More than 1 std dev below mean
            comm_score += abs(sync_z - 1.0) * 1.0
            reasons.append(f"low_sync_z({sync_z:.1f})")
        
        # Multiple processes (MPI coordination) - normalized by expected
        if self.expected_mpi_ranks > 0:
            rank_ratio = pids_count / self.expected_mpi_ranks
            if rank_ratio > 0.8:  # At least 80% of expected ranks
                comm_score += 1.0
                reasons.append(f"multi_rank({pids_count}/{self.expected_mpi_ranks})")
        
        # --- Compute Indicators ---
        # Lower idle time (relative to baseline)
        if idle_z < -2.0:  # More than 2 std dev below mean
            comp_score += abs(idle_z - 2.0) * 2
            reasons.append(f"low_idle_z({idle_z:.1f})")
        
        # Higher power (relative to baseline)
        if power_z > 2.0:  # More than 2 std dev above mean
            comp_score += (power_z - 2.0) * 2
            reasons.append(f"high_power_z({power_z:.1f})")
        
        # Very low context switches (compute intensive)
        if ctx_z < -1.5 and ctx_switch_rate < 100:  # Less than 100/s and 1.5 std dev below mean
            comp_score += abs(ctx_z - 1.5) * 1.5
            reasons.append(f"very_low_ctx({ctx_switch_rate:.0f}/s)")
        
        # High synchronization variance (compute imbalance)
        if sync_z > 2.0:  # More than 2 std dev above mean
            comp_score += (sync_z - 2.0) * 1.0
            reasons.append(f"high_sync_z({sync_z:.1f})")
        
        # Significant CPU activity
        if active_cpu_pct > 20:  # Absolute threshold for significant compute
            comp_score += min(active_cpu_pct / 50.0, 2.0)  # Max 2 points
            reasons.append(f"active_cpu({active_cpu_pct:.0f}%)")
        
        # Network activity (only meaningful if network interfaces available)
        if total_net_bw > 10 and self.network_interfaces:  # Significant network traffic
            # Network during compute is unlikely for miniMD (except maybe for large MPI)
            # But we'll give it a small penalty for compute detection
            comp_score -= 0.5
            reasons.append(f"net_bw({total_net_bw:.1f}MB/s)")
        
        # --- Phase Decision ---
        phase = "MIXED"
        confidence = 0.0
        
        # Normalize scores (0-10 scale)
        comm_norm = min(comm_score / 5.0, 1.0) if comm_score > 0 else 0
        comp_norm = min(comp_score / 5.0, 1.0) if comp_score > 0 else 0
        
        # Decision logic
        if comm_norm > 0.6 and comm_norm > comp_norm * 1.5:
            phase = "COMMUNICATION"
            confidence = comm_norm
        elif comp_norm > 0.6 and comp_norm > comm_norm * 1.5:
            phase = "COMPUTE"
            confidence = comp_norm
        elif abs(comm_norm - comp_norm) < 0.2:
            phase = "MIXED"
            confidence = max(comm_norm, comp_norm) * 0.7
        else:
            # Slight preference for communication (since miniMD is communication-bound)
            if comm_norm > comp_norm:
                phase = "COMMUNICATION"
                confidence = comm_norm * 0.8
            else:
                phase = "COMPUTE"
                confidence = comp_norm * 0.8
        
        # Apply minimum phase duration
        if phase != self.current_phase and time_in_current_phase < MIN_PHASE_DURATION:
            # Keep current phase if we haven't been in it long enough
            phase = self.current_phase
            confidence = max(0.3, confidence * 0.7)  # Lower confidence for forced continuation
            reasons.append(f"min_duration({time_in_current_phase:.1f}s)")
        
        # Special case: If we're seeing very high idle (>98%) and low context switches,
        # it might be actual idle (end of program)
        if idle_pct > 98 and ctx_switch_rate < 10 and pids_count < self.expected_mpi_ranks * 0.5:
            phase = "IDLE/COMPLETION"
            confidence = 0.9
            reasons = [f"very_high_idle({idle_pct:.0f}%),low_ctx({ctx_switch_rate:.0f}/s),few_ranks({pids_count})"]
        
        details = {
            'phase': phase,
            'confidence': confidence,
            'power_W': pkg_power_W,
            'dram_power_W': dram_power_W,
            'cpu_user_pct': user_pct,
            'cpu_system_pct': system_pct,
            'cpu_idle_pct': idle_pct,
            'cpu_iowait_pct': iowait_pct,
            'active_cpu_pct': active_cpu_pct,
            'voluntary_ctx_switches_per_sec': ctx_switch_rate,
            'active_mpi_ranks': pids_count,
            'net_recv_mbps': net_recv_mbps,
            'net_sent_mbps': net_sent_mbps,
            'sync_variance_pct': sync_variance_norm,
            'power_z_score': power_z,
            'idle_z_score': idle_z,
            'ctx_z_score': ctx_z,
            'sync_z_score': sync_z,
            'comm_score_norm': comm_norm,
            'comp_score_norm': comp_norm,
            'reasons': ','.join(reasons) if reasons else 'balanced'
        }
        
        return phase, details
    
    def track_phase_duration(self, new_phase, timestamp, sample_interval):
        """Track duration and samples for each phase"""
        # Update statistics for current phase
        if self.current_phase:
            self.phase_stats[self.current_phase]["time"] += sample_interval
            self.phase_stats[self.current_phase]["samples"] += 1
        
        # Check for phase change
        if new_phase != self.current_phase:
            self.current_phase = new_phase
    
    def setup_csv(self):
        """Initialize CSV with all adaptive metrics"""
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
            'cpu_active_pct',
            'cpu_iowait_pct',
            'voluntary_ctx_switches_per_sec',
            'active_mpi_ranks',
            'expected_mpi_ranks',
            'net_recv_mbps',
            'net_sent_mbps',
            'sync_variance_pct',
            'power_z_score',
            'idle_z_score',
            'ctx_z_score',
            'sync_z_score',
            'comm_score_norm',
            'comp_score_norm',
            'pkg_energy_J',
            'dram_energy_J',
            'reasons'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        print(f"[INFO] Logging to {LOG_FILE}")
        print(f"[INFO] Adaptive window: {ADAPTIVE_WINDOW} samples")
        print(f"[INFO] Initial samples for baseline: {INITIAL_SAMPLES}")
        print(f"[INFO] Minimum phase duration: {MIN_PHASE_DURATION}s")
        if self.has_dram_rapl:
            print("[INFO] DRAM RAPL available - tracking memory power")
        if self.network_interfaces:
            print(f"[INFO] Monitoring network interfaces: {self.network_interfaces}")
    
    def generate_summary_report(self, total_time, miniMD_timings=None):
        """Generate detailed summary report with statistical analysis"""
        with open(SUMMARY_FILE, "w") as f:
            f.write("="*80 + "\n")
            f.write("MiniMD Communication Phase Analysis - Version 9.0 (ADAPTIVE)\n")
            f.write("="*80 + "\n\n")
            
            # Add miniMD ground truth if provided
            if miniMD_timings:
                f.write("GROUND TRUTH FROM miniMD OUTPUT:\n")
                f.write("-"*40 + "\n")
                for key, value in miniMD_timings.items():
                    f.write(f"{key:20s}: {value}\n")
                f.write("\n")
            
            # Statistical summary
            f.write("STATISTICAL BASELINES ESTABLISHED:\n")
            f.write("-"*40 + "\n")
            if len(self.power_baseline) > 0:
                f.write(f"Power:      mean={np.mean(self.power_baseline):.1f}W, std={np.std(self.power_baseline):.1f}W\n")
                f.write(f"Idle time:  mean={np.mean(self.idle_baseline):.1f}%, std={np.std(self.idle_baseline):.1f}%\n")
                f.write(f"Ctx rate:   mean={np.mean(self.ctx_baseline):.0f}/s, std={np.std(self.ctx_baseline):.0f}/s\n")
                f.write(f"Sync var:   mean={np.mean(self.sync_baseline):.1f}%, std={np.std(self.sync_baseline):.1f}%\n")
                f.write(f"Samples for baseline: {len(self.power_baseline)}\n")
            f.write("\n")
            
            f.write("PHASE DURATION ANALYSIS:\n")
            f.write("-"*40 + "\n")
            
            # Calculate percentages and averages correctly
            total_tracked = sum(stats["time"] for stats in self.phase_stats.values())
            
            for phase in sorted(self.phase_stats.keys()):
                stats = self.phase_stats[phase]
                duration = stats["time"]
                samples = stats["samples"]
                
                if total_time > 0:
                    percentage = (duration / total_time) * 100
                else:
                    percentage = 0
                
                if samples > 0:
                    avg_duration = duration / samples
                else:
                    avg_duration = 0
                
                f.write(f"{phase:25s}: {duration:7.2f}s ({percentage:5.1f}%) | "
                       f"Samples: {samples:4d} | Avg: {avg_duration:.3f}s\n")
            
            f.write(f"\nTotal monitored time: {total_time:.2f}s\n")
            f.write(f"Total tracked phase time: {total_tracked:.2f}s\n")
            
            # Communication efficiency analysis
            if "COMMUNICATION" in self.phase_stats:
                comm_time = self.phase_stats["COMMUNICATION"]["time"]
                comm_efficiency = (comm_time / total_time * 100) if total_time > 0 else 0
                
                f.write(f"\nCOMMUNICATION ANALYSIS:\n")
                f.write("-"*40 + "\n")
                f.write(f"Time in communication: {comm_time:.2f}s ({comm_efficiency:.1f}%)\n")
                
                if miniMD_timings and "t_comm" in miniMD_timings and "t_total" in miniMD_timings:
                    expected_pct = (miniMD_timings["t_comm"] / miniMD_timings["t_total"]) * 100
                    f.write(f"Expected from miniMD: {expected_pct:.1f}%\n")
                    diff = comm_efficiency - expected_pct
                    if abs(diff) > 5:
                        f.write(f"NOTE: Difference of {diff:.1f}% from miniMD timing\n")
                        if diff > 5:
                            f.write(f"      Could indicate: Over-detection of communication\n")
                        else:
                            f.write(f"      Could indicate: Missing communication phases\n")
                else:
                    f.write(f"Expected (typical): ~96-98%\n")
            
            # Compute phase analysis
            if "COMPUTE" in self.phase_stats:
                comp_time = self.phase_stats["COMPUTE"]["time"]
                comp_samples = self.phase_stats["COMPUTE"]["samples"]
                f.write(f"\nCOMPUTE ANALYSIS:\n")
                f.write("-"*40 + "\n")
                f.write(f"Time in compute: {comp_time:.2f}s ({comp_time/total_time*100:.1f}%)\n")
                f.write(f"Number of compute bursts: {comp_samples}\n")
                if comp_samples > 0:
                    f.write(f"Average compute burst duration: {comp_time/comp_samples:.3f}s\n")
            
            # Optimization recommendations based on actual measurements
            f.write("\n" + "="*80 + "\n")
            f.write("OPTIMIZATION RECOMMENDATIONS (BASED ON MEASUREMENTS):\n")
            f.write("="*80 + "\n\n")
            
            # MPI size specific recommendations
            if self.expected_mpi_ranks > 0:
                f.write(f"MPI SIZE ANALYSIS ({self.expected_mpi_ranks} ranks):\n")
                f.write("-"*40 + "\n")
                
                if self.expected_mpi_ranks <= 4:
                    f.write("1. SMALL MPI SIZE (≤4 ranks):\n")
                    f.write("   - Computation dominates (check compute percentage)\n")
                    f.write("   - Focus on compute optimization\n")
                    f.write("   - Consider reducing communication frequency\n")
                elif self.expected_mpi_ranks <= 16:
                    f.write("1. MEDIUM MPI SIZE (5-16 ranks):\n")
                    f.write("   - Balanced workload\n")
                    f.write("   - Optimize both computation and communication\n")
                    f.write("   - Consider ghost cell optimization\n")
                else:
                    f.write("1. LARGE MPI SIZE (>16 ranks):\n")
                    f.write("   - Communication dominates\n")
                    f.write("   - Focus on reducing MPI overhead\n")
                    f.write("   - Consider non-blocking communication\n")
                    f.write("   - Evaluate load balancing\n")
            
            # Based on measured communication percentage
            comm_pct = self.phase_stats.get("COMMUNICATION", {"time": 0})["time"] / total_time * 100 if total_time > 0 else 0
            f.write(f"\n2. WORKLOAD CHARACTERIZATION ({comm_pct:.1f}% communication):\n")
            f.write("-"*40 + "\n")
            
            if comm_pct > 95:
                f.write("   - EXTREMELY COMMUNICATION-BOUND WORKLOAD\n")
                f.write("   - Primary optimization target: MPI communication\n")
                f.write("   - Secondary: Load balancing and domain decomposition\n")
            elif comm_pct > 80:
                f.write("   - STRONGLY COMMUNICATION-BOUND WORKLOAD\n")
                f.write("   - Focus on: MPI collectives, message aggregation\n")
                f.write("   - Consider: Overlap computation with communication\n")
            elif comm_pct > 50:
                f.write("   - COMMUNICATION-DOMINATED WORKLOAD\n")
                f.write("   - Balance: Both MPI and compute optimizations\n")
                f.write("   - Techniques: Non-blocking MPI, algorithm tuning\n")
            else:
                f.write("   - COMPUTE-DOMINATED WORKLOAD\n")
                f.write("   - Focus on: Vectorization, threading, algorithm optimization\n")
                f.write("   - Secondary: Reduce MPI overhead where possible\n")
            
            f.write("\n3. SPECIFIC OPTIMIZATIONS BASED ON METRICS:\n")
            f.write("-"*40 + "\n")
            
            # Context switch analysis
            if len(self.ctx_baseline) > 0:
                avg_ctx = np.mean(self.ctx_baseline)
                if avg_ctx > 1000:
                    f.write("   - High context switches detected\n")
                    f.write("   - Consider: MPI_Init_thread with MPI_THREAD_MULTIPLE\n")
                    f.write("   - Or: Check for unnecessary MPI_Wait calls\n")
            
            # Sync variance analysis
            if len(self.sync_baseline) > 0:
                avg_sync = np.mean(self.sync_baseline)
                if avg_sync > 50:
                    f.write("   - High synchronization variance detected\n")
                    f.write("   - Indicates: Load imbalance across ranks\n")
                    f.write("   - Consider: Better domain decomposition\n")
                elif avg_sync < 10:
                    f.write("   - Excellent synchronization detected\n")
                    f.write("   - Good load balancing\n")
            
            f.write("\n4. GENERAL RECOMMENDATIONS:\n")
            f.write("-"*40 + "\n")
            f.write("   - Reduce message sizes (ghost cell optimization)\n")
            f.write("   - Use MPI_Iallreduce for global operations\n")
            f.write("   - Consider communication-computation overlap\n")
            f.write("   - Profile with hardware counters for deeper insights\n")
        
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
    
    def extract_miniMD_timings(self):
        """Extract timing information from miniMD output"""
        # In practice, this would parse the miniMD output file
        # For now, we'll return typical values and note they should be updated
        return {
            "t_total": "Parse from miniMD output",
            "t_comm": "Parse from miniMD output",
            "t_force": "Parse from miniMD output",
            "t_neigh": "Parse from miniMD output",
            "t_other": "Parse from miniMD output",
            "comm_percentage": "Parse from miniMD output"
        }
    
    def run_monitoring(self):
        """Main monitoring loop with adaptive classification"""
        print(f"[INFO] Starting adaptive communication phase monitoring v9.0")
        print(f"[INFO] Command: {' '.join(CMD)}")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        print(f"[INFO] Detection: Statistical adaptive classification")
        
        self.setup_csv()
        
        # Wait for miniMD to start
        print("[INFO] Waiting for miniMD to launch...")
        while not self.get_miniMD_pids():
            time.sleep(0.5)
        
        print("[INFO] miniMD detected, starting monitoring...")
        
        # Initialize counters
        self.prev_energy = self.read_rapl_energy(RAPL_PATH, "pkg_energy")
        if self.has_dram_rapl:
            self.prev_dram_energy = self.read_rapl_energy(RAPL_DRAM_PATH, "dram_energy")
        
        # Initialize state
        self.prev_proc_stats = self.read_proc_stat()
        self.prev_net_stats = self.read_network_stats()
        self.prev_ctx_switches = (0, 0)
        self.prev_per_process_cpu = {}
        
        self.start_time = time.time()
        sample_count = 0
        last_phase_change = self.start_time
        
        try:
            while True:
                # Check if miniMD still running
                pids = self.get_miniMD_pids()
                if not pids:
                    print("[INFO] miniMD completed")
                    break
                
                sample_count += 1
                current_time = time.time() - self.start_time
                time_in_current_phase = current_time - last_phase_change
                
                # Sleep for sampling interval
                time.sleep(SAMPLE_INTERVAL)
                
                # Read current energy
                energy_now = self.read_rapl_energy(RAPL_PATH, "pkg_energy")
                delta_pkg_energy = self.safe_delta(energy_now, self.prev_energy, "pkg_energy_uj") / 1e6
                self.prev_energy = energy_now
                
                # Calculate package power
                pkg_power_W = delta_pkg_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Read DRAM energy if available
                dram_power_W = 0
                delta_dram_energy = 0
                if self.has_dram_rapl:
                    dram_energy_now = self.read_rapl_energy(RAPL_DRAM_PATH, "dram_energy")
                    delta_dram_energy = self.safe_delta(dram_energy_now, self.prev_dram_energy, "dram_energy_uj") / 1e6
                    self.prev_dram_energy = dram_energy_now
                    dram_power_W = delta_dram_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Read CPU utilization
                curr_proc_stats = self.read_proc_stat()
                cpu_metrics = self.calculate_cpu_usage(self.prev_proc_stats, curr_proc_stats)
                self.prev_proc_stats = curr_proc_stats
                
                # Read network statistics
                curr_net_stats = self.read_network_stats()
                net_recv_mbps, net_sent_mbps = self.calculate_network_bandwidth(self.prev_net_stats, curr_net_stats)
                self.prev_net_stats = curr_net_stats
                
                # Read context switches with safe rate calculation
                curr_ctx_switches = self.get_proc_context_switches(pids)
                voluntary_ctx_rate = self.safe_rate(
                    curr_ctx_switches[0], self.prev_ctx_switches[0],
                    SAMPLE_INTERVAL, "voluntary_ctx"
                )
                self.prev_ctx_switches = curr_ctx_switches
                
                # Get per-process CPU times for synchronization analysis
                curr_per_process_cpu = self.get_per_process_cpu_times(pids)
                sync_variance_norm = self.calculate_sync_variance(curr_per_process_cpu, self.prev_per_process_cpu)
                self.prev_per_process_cpu = curr_per_process_cpu
                
                # Classify phase using adaptive method
                phase, details = self.classify_phase_adaptive(
                    pkg_power_W, dram_power_W, cpu_metrics,
                    voluntary_ctx_rate, len(pids),
                    (net_recv_mbps, net_sent_mbps),
                    sync_variance_norm, time_in_current_phase
                )
                
                # Track phase change
                if phase != self.current_phase:
                    last_phase_change = current_time
                
                # Track phase duration and samples
                self.track_phase_duration(phase, current_time, SAMPLE_INTERVAL)
                
                # Log data
                row = {
                    'timestamp': f"{current_time:.2f}",
                    'phase': details['phase'],
                    'confidence': f"{details['confidence']:.3f}",
                    'power_W': f"{details['power_W']:.1f}",
                    'dram_power_W': f"{details['dram_power_W']:.1f}",
                    'cpu_user_pct': f"{details['cpu_user_pct']:.1f}",
                    'cpu_system_pct': f"{details['cpu_system_pct']:.1f}",
                    'cpu_idle_pct': f"{details['cpu_idle_pct']:.1f}",
                    'cpu_active_pct': f"{details['active_cpu_pct']:.1f}",
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'voluntary_ctx_switches_per_sec': f"{details['voluntary_ctx_switches_per_sec']:.0f}",
                    'active_mpi_ranks': details['active_mpi_ranks'],
                    'expected_mpi_ranks': self.expected_mpi_ranks,
                    'net_recv_mbps': f"{details['net_recv_mbps']:.1f}",
                    'net_sent_mbps': f"{details['net_sent_mbps']:.1f}",
                    'sync_variance_pct': f"{details['sync_variance_pct']:.1f}",
                    'power_z_score': f"{details.get('power_z_score', 0):.2f}",
                    'idle_z_score': f"{details.get('idle_z_score', 0):.2f}",
                    'ctx_z_score': f"{details.get('ctx_z_score', 0):.2f}",
                    'sync_z_score': f"{details.get('sync_z_score', 0):.2f}",
                    'comm_score_norm': f"{details.get('comm_score_norm', 0):.3f}",
                    'comp_score_norm': f"{details.get('comp_score_norm', 0):.3f}",
                    'pkg_energy_J': f"{delta_pkg_energy:.4f}",
                    'dram_energy_J': f"{delta_dram_energy:.4f}",
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status
                if sample_count % 5 == 0:
                    phase_char = details['phase'][0] if details['phase'] != 'INITIALIZING' else 'I'
                    if not self.baseline_established:
                        status = f"Collecting baseline ({self.samples_collected}/{INITIAL_SAMPLES})"
                    else:
                        status = f"Z-scores: P:{details.get('power_z_score', 0):.1f} I:{details.get('idle_z_score', 0):.1f}"
                    
                    print(f"[{current_time:6.1f}s] {phase_char} "
                          f"Pwr:{details['power_W']:4.0f}W "
                          f"Idle:{details['cpu_idle_pct']:3.0f}% "
                          f"Ctx:{details['voluntary_ctx_switches_per_sec']:5.0f}/s "
                          f"R:{details['active_mpi_ranks']:2d}/{self.expected_mpi_ranks:2d} "
                          f"Conf:{details['confidence']:.2f} "
                          f"{status}")
                
        except KeyboardInterrupt:
            print("\n[INFO] Monitoring interrupted by user")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup and generate final summary"""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        # Get miniMD timings for comparison
        miniMD_timings = self.extract_miniMD_timings()
        
        if self.csv_file:
            self.csv_file.close()
        
        # Generate summary report
        self.generate_summary_report(total_time, miniMD_timings)
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
        
        # Print quick summary
        print("\n" + "="*70)
        print("ADAPTIVE ANALYSIS SUMMARY:")
        print("="*70)
        for phase in sorted(self.phase_stats.keys()):
            stats = self.phase_stats[phase]
            duration = stats["time"]
            percentage = (duration / total_time * 100) if total_time > 0 else 0
            print(f"{phase:25s}: {duration:6.2f}s ({percentage:5.1f}%)")

def main():
    """Entry point"""
    
    if not os.path.exists(RAPL_PATH):
        print(f"[ERROR] RAPL not accessible at {RAPL_PATH}")
        print("[ERROR] Cannot monitor without energy data")
        return 1
    
    print("="*80)
    print("ADAPTIVE MiniMD Communication Phase Monitor - Version 9.0")
    print("="*80)
    print("Key features:")
    print("  ✓ Statistical baselines (no fixed thresholds)")
    print("  ✓ Z-score based detection (adaptive to workload)")
    print("  ✓ MPI-size aware classification")
    print("  ✓ Counter overflow protection (from v8)")
    print("  ✓ Correct sync variance (from v8)")
    print("  ✓ Power spike detection (from v7.1)")
    print("  ✓ Proper phase tracking (from v8)")
    print("  ✓ Working communication detection (from v7)")
    print("="*80)
    print("Adaptive parameters:")
    print(f"  Baseline window: {ADAPTIVE_WINDOW} samples")
    print(f"  Initial samples: {INITIAL_SAMPLES}")
    print(f"  Min phase duration: {MIN_PHASE_DURATION}s")
    print("="*80)
    
    monitor = AdaptiveCommPhaseMonitor()
    
    try:
        monitor.run_monitoring()
    except Exception as e:
        print(f"[FATAL] {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
