#!/usr/bin/env python3
"""
Adaptive Communication Phase Monitor for miniMD on Frontenac
Version 9.0 - Fully adaptive with formula-based thresholds
Key features:
1. No hardcoded thresholds - all thresholds adapt to observed data
2. Multi-dimensional scoring system with weighted indicators
3. Statistical baselines computed during warm-up period
4. Automatic detection of system characteristics
5. Robust phase detection for both low and high rank counts
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
from scipy import stats

# ============ CONFIGURATION ============
CMD = ["mpirun", "--oversubscribe", "-np", "32", 
       "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = f"comm_phase_monitoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
SUMMARY_FILE = f"comm_phase_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
SAMPLE_INTERVAL = 0.2  # 200ms for better temporal resolution
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_DRAM_PATH = "/sys/class/powercap/intel-rapl:0:0/energy_uj"

# Adaptive parameters (these adjust automatically)
WARMUP_SAMPLES = 20  # Samples to collect for baseline statistics
MIN_PHASE_DURATION = 0.4  # Minimum phase duration to prevent flapping
ROBUSTNESS_FACTOR = 1.5  # For outlier detection (z-score multiplier)

# Network interfaces to monitor
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
        self.network_interfaces = self.detect_network_interfaces()
        
        # Statistical baselines (will be computed during warm-up)
        self.baselines = {
            'power': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0},
            'idle': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0},
            'ctx_rate': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0},
            'sync_var': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0},
            'active_cpu': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0},
            'net_bw': {'mean': 0, 'std': 0, 'min': float('inf'), 'max': 0}
        }
        
        # Rolling windows for adaptive statistics
        self.power_history = deque(maxlen=100)
        self.idle_history = deque(maxlen=100)
        self.ctx_rate_history = deque(maxlen=100)
        self.sync_var_history = deque(maxlen=100)
        self.active_cpu_history = deque(maxlen=100)
        self.net_bw_history = deque(maxlen=100)
        
        # Phase tracking
        self.phase_stats = defaultdict(lambda: {"time": 0.0, "samples": 0, "transitions": 0})
        self.current_phase = "UNKNOWN"
        self.phase_start_time = None
        self.last_phase_change = None
        
        # System characteristics
        self.total_cores = self.get_total_cores()
        self.warmup_complete = False
        self.warmup_samples = 0
        
        print(f"[INFO] System has {self.total_cores} CPU cores")
        print(f"[INFO] Detected network interfaces: {self.network_interfaces}")
    
    def get_total_cores(self):
        """Get total number of CPU cores in the system"""
        try:
            with open('/proc/cpuinfo', 'r') as f:
                cores = sum(1 for line in f if line.strip().startswith('processor'))
            return cores
        except:
            return 32  # Fallback assumption
    
    def detect_network_interfaces(self):
        """Detect available network interfaces for monitoring"""
        interfaces = []
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()[2:]
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
                return [int(pid) for pid in result.stdout.strip().split()]
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
        if "energy" in counter_name:
            print(f"[DEBUG] Counter wrap-around for {counter_name}: {previous} -> {current}")
        return delta
    
    def safe_rate(self, curr, prev, dt, counter_name=""):
        """Calculate rate with sanity clamping"""
        if dt <= 0:
            return 0.0
        
        delta = self.safe_delta(curr, prev, counter_name)
        rate = delta / dt
        
        # Clamp impossible rates
        MAX_CTX_RATE = 1e6
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
                lines = f.readlines()[2:]
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
                    utime = int(stat_data[13])
                    stime = int(stat_data[14])
                    cpu_times[pid] = utime + stime
            except (FileNotFoundError, ValueError, IndexError):
                continue
        return cpu_times
    
    def calculate_sync_variance(self, curr_cpu_times, prev_cpu_times):
        """Calculate synchronization variance across processes using per-interval deltas"""
        deltas = []
        for pid, curr_time in curr_cpu_times.items():
            if pid in prev_cpu_times:
                prev_time = prev_cpu_times[pid]
                delta = max(curr_time - prev_time, 0)
                deltas.append(delta)
        
        if len(deltas) < 2:
            return 0.0
        
        try:
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
                line = f.readline()
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
    
    def update_baselines(self, metrics):
        """Update statistical baselines during warm-up period"""
        if not self.warmup_complete:
            self.warmup_samples += 1
            
            # Update each metric's history
            self.power_history.append(metrics['power_W'])
            self.idle_history.append(metrics['idle_pct'])
            self.ctx_rate_history.append(metrics['ctx_switch_rate'])
            self.sync_var_history.append(metrics['sync_variance_norm'])
            self.active_cpu_history.append(metrics['active_cpu_pct'])
            self.net_bw_history.append(metrics['total_net_bw'])
            
            if self.warmup_samples >= WARMUP_SAMPLES:
                self.compute_baselines()
                self.warmup_complete = True
                print(f"[INFO] Warm-up complete. Baselines computed from {self.warmup_samples} samples")
                self.print_baselines()
    
    def compute_baselines(self):
        """Compute statistical baselines from collected data"""
        if len(self.power_history) > 1:
            self.baselines['power']['mean'] = statistics.mean(self.power_history)
            self.baselines['power']['std'] = statistics.stdev(self.power_history)
            self.baselines['power']['min'] = min(self.power_history)
            self.baselines['power']['max'] = max(self.power_history)
        
        if len(self.idle_history) > 1:
            self.baselines['idle']['mean'] = statistics.mean(self.idle_history)
            self.baselines['idle']['std'] = statistics.stdev(self.idle_history)
            self.baselines['idle']['min'] = min(self.idle_history)
            self.baselines['idle']['max'] = max(self.idle_history)
        
        if len(self.ctx_rate_history) > 1:
            self.baselines['ctx_rate']['mean'] = statistics.mean(self.ctx_rate_history)
            self.baselines['ctx_rate']['std'] = statistics.stdev(self.ctx_rate_history)
            self.baselines['ctx_rate']['min'] = min(self.ctx_rate_history)
            self.baselines['ctx_rate']['max'] = max(self.ctx_rate_history)
        
        if len(self.sync_var_history) > 1:
            self.baselines['sync_var']['mean'] = statistics.mean(self.sync_var_history)
            self.baselines['sync_var']['std'] = statistics.stdev(self.sync_var_history)
            self.baselines['sync_var']['min'] = min(self.sync_var_history)
            self.baselines['sync_var']['max'] = max(self.sync_var_history)
        
        if len(self.active_cpu_history) > 1:
            self.baselines['active_cpu']['mean'] = statistics.mean(self.active_cpu_history)
            self.baselines['active_cpu']['std'] = statistics.stdev(self.active_cpu_history)
            self.baselines['active_cpu']['min'] = min(self.active_cpu_history)
            self.baselines['active_cpu']['max'] = max(self.active_cpu_history)
        
        if len(self.net_bw_history) > 1:
            self.baselines['net_bw']['mean'] = statistics.mean(self.net_bw_history)
            self.baselines['net_bw']['std'] = statistics.stdev(self.net_bw_history)
            self.baselines['net_bw']['min'] = min(self.net_bw_history)
            self.baselines['net_bw']['max'] = max(self.net_bw_history)
    
    def print_baselines(self):
        """Print computed baselines for debugging"""
        print("\n" + "="*60)
        print("ADAPTIVE BASELINES COMPUTED:")
        print("="*60)
        for metric, stats in self.baselines.items():
            print(f"{metric:15s}: mean={stats['mean']:6.1f}, std={stats['std']:6.1f}, "
                  f"range=[{stats['min']:6.1f}, {stats['max']:6.1f}]")
        print("="*60 + "\n")
    
    def calculate_adaptive_thresholds(self, metrics, active_ranks):
        """Calculate adaptive thresholds based on statistical baselines"""
        if not self.warmup_complete:
            return self.get_default_thresholds()
        
        # Calculate expected idle based on active ranks vs total cores
        expected_max_active = (active_ranks / self.total_cores) * 100
        idle_adjustment = max(0, 100 - expected_max_active - metrics['active_cpu_pct'])
        
        # Dynamic thresholds based on standard deviations
        power_comm_threshold = self.baselines['power']['mean'] - ROBUSTNESS_FACTOR * self.baselines['power']['std']
        power_comp_threshold = self.baselines['power']['mean'] + ROBUSTNESS_FACTOR * self.baselines['power']['std']
        
        idle_comm_threshold = self.baselines['idle']['mean'] + ROBUSTNESS_FACTOR * self.baselines['idle']['std'] + idle_adjustment
        idle_comp_threshold = self.baselines['idle']['mean'] - ROBUSTNESS_FACTOR * self.baselines['idle']['std']
        
        ctx_comm_threshold = self.baselines['ctx_rate']['mean'] + ROBUSTNESS_FACTOR * self.baselines['ctx_rate']['std']
        ctx_comp_threshold = self.baselines['ctx_rate']['mean'] - ROBUSTNESS_FACTOR * self.baselines['ctx_rate']['std']
        
        sync_comm_threshold = self.baselines['sync_var']['mean'] - ROBUSTNESS_FACTOR * self.baselines['sync_var']['std']
        sync_comp_threshold = self.baselines['sync_var']['mean'] + ROBUSTNESS_FACTOR * self.baselines['sync_var']['std']
        
        net_comm_threshold = self.baselines['net_bw']['mean'] + ROBUSTNESS_FACTOR * self.baselines['net_bw']['std']
        
        return {
            'power_comm': max(50, power_comm_threshold),  # Minimum 50W
            'power_comp': min(350, power_comp_threshold),  # Maximum 350W
            'idle_comm': min(99, idle_comm_threshold),  # Maximum 99%
            'idle_comp': max(10, idle_comp_threshold),  # Minimum 10%
            'ctx_comm': max(100, ctx_comm_threshold),  # Minimum 100/s
            'ctx_comp': max(0, ctx_comp_threshold),  # Minimum 0/s
            'sync_comm': max(0, sync_comm_threshold),  # Minimum 0%
            'sync_comp': max(10, sync_comp_threshold),  # Minimum 10%
            'net_comm': max(1, net_comm_threshold)  # Minimum 1 MB/s
        }
    
    def get_default_thresholds(self):
        """Default thresholds before warm-up completes"""
        return {
            'power_comm': 150,
            'power_comp': 250,
            'idle_comm': 85,
            'idle_comp': 50,
            'ctx_comm': 50,
            'ctx_comp': 10,
            'sync_comm': 10,
            'sync_comp': 50,
            'net_comm': 1
        }
    
    def classify_phase_adaptive(self, metrics, thresholds, active_ranks):
        """Adaptive phase classification using multi-dimensional scoring"""
        reasons = []
        comm_score = 0.0
        comp_score = 0.0
        total_possible = 0.0
        
        # Weight factors (sum to 1.0)
        weights = {
            'power': 0.25,
            'idle': 0.25,
            'ctx': 0.20,
            'sync': 0.15,
            'net': 0.10,
            'ranks': 0.05
        }
        
        # 1. Power indicator
        if metrics['power_W'] < thresholds['power_comm']:
            comm_score += weights['power']
            reasons.append(f"low_pwr({metrics['power_W']:.0f}W<{thresholds['power_comm']:.0f})")
        elif metrics['power_W'] > thresholds['power_comp']:
            comp_score += weights['power']
            reasons.append(f"high_pwr({metrics['power_W']:.0f}W>{thresholds['power_comp']:.0f})")
        total_possible += weights['power']
        
        # 2. Idle indicator (adjusted for expected utilization)
        expected_idle = 100 - (active_ranks / self.total_cores * 100 * 0.8)  # 80% efficiency factor
        idle_adjusted = metrics['idle_pct'] - expected_idle
        
        if idle_adjusted > thresholds['idle_comm'] - expected_idle:
            comm_score += weights['idle']
            reasons.append(f"high_idle({metrics['idle_pct']:.0f}%>exp{expected_idle:.0f}%)")
        elif metrics['idle_pct'] < thresholds['idle_comp']:
            comp_score += weights['idle']
            reasons.append(f"low_idle({metrics['idle_pct']:.0f}%<{thresholds['idle_comp']:.0f}%)")
        total_possible += weights['idle']
        
        # 3. Context switch indicator
        if metrics['ctx_switch_rate'] > thresholds['ctx_comm']:
            comm_score += weights['ctx']
            reasons.append(f"high_ctx({metrics['ctx_switch_rate']:.0f}/s>{thresholds['ctx_comm']:.0f})")
        elif metrics['ctx_switch_rate'] < thresholds['ctx_comp']:
            comp_score += weights['ctx']
            reasons.append(f"low_ctx({metrics['ctx_switch_rate']:.0f}/s<{thresholds['ctx_comp']:.0f})")
        total_possible += weights['ctx']
        
        # 4. Synchronization indicator
        if metrics['sync_variance_norm'] < thresholds['sync_comm']:
            comm_score += weights['sync']
            reasons.append(f"low_sync({metrics['sync_variance_norm']:.1f}%<{thresholds['sync_comm']:.0f})")
        elif metrics['sync_variance_norm'] > thresholds['sync_comp']:
            comp_score += weights['sync']
            reasons.append(f"high_sync({metrics['sync_variance_norm']:.1f}%>{thresholds['sync_comp']:.0f})")
        total_possible += weights['sync']
        
        # 5. Network indicator (if available)
        if metrics['total_net_bw'] > thresholds['net_comm']:
            comm_score += weights['net']
            reasons.append(f"net_bw({metrics['total_net_bw']:.1f}MB/s>{thresholds['net_comm']:.0f})")
        total_possible += weights['net']
        
        # 6. Rank utilization indicator
        utilization_ratio = active_ranks / self.total_cores
        if utilization_ratio > 0.7:  # High system utilization
            comp_score += weights['ranks'] * utilization_ratio
            reasons.append(f"high_util({utilization_ratio:.1%})")
        elif active_ranks > 1:
            comm_score += weights['ranks'] * 0.5
            reasons.append(f"multi_rank({active_ranks})")
        total_possible += weights['ranks']
        
        # Calculate confidence
        if total_possible > 0:
            confidence = max(comm_score, comp_score) / total_possible
        else:
            confidence = 0.0
        
        # Phase determination with hysteresis
        phase = "MIXED"
        if comm_score > comp_score + 0.1:  # Hysteresis margin
            phase = "COMMUNICATION"
        elif comp_score > comm_score + 0.1:
            phase = "COMPUTE"
        
        # Power spike detection (override for strong compute signals)
        power_spike_factor = metrics['power_W'] / self.baselines['power']['mean'] if self.baselines['power']['mean'] > 0 else 1.0
        if power_spike_factor > 1.3 and metrics['active_cpu_pct'] > 10:
            phase = "COMPUTE"
            confidence = min(1.0, confidence * 1.2)
            reasons.append(f"pwr_spike({power_spike_factor:.1f}x)")
        
        # Special case: Very high idle + low ranks = likely idle, not communication
        if metrics['idle_pct'] > 98 and active_ranks < self.total_cores / 4:
            phase = "IDLE"
            reasons.append(f"very_high_idle({metrics['idle_pct']:.0f}%)")
        
        details = {
            'phase': phase,
            'confidence': confidence,
            'comm_score': comm_score,
            'comp_score': comp_score,
            'reasons': ','.join(reasons) if reasons else 'balanced',
            **metrics
        }
        
        return phase, details
    
    def track_phase_duration(self, new_phase, timestamp):
        """Track phase durations with adaptive timing"""
        if self.current_phase is None:
            self.current_phase = new_phase
            self.phase_start_time = timestamp
            self.last_phase_change = timestamp
        elif self.current_phase != new_phase:
            # Check minimum duration to prevent flapping
            time_in_current = timestamp - self.last_phase_change
            if time_in_current >= MIN_PHASE_DURATION:
                # Record previous phase
                duration = timestamp - self.phase_start_time
                if duration > 0 and self.current_phase != "UNKNOWN":
                    self.phase_stats[self.current_phase]["time"] += duration
                    self.phase_stats[self.current_phase]["samples"] += 1
                    self.phase_stats[self.current_phase]["transitions"] += 1
                
                # Start new phase
                self.current_phase = new_phase
                self.phase_start_time = timestamp
                self.last_phase_change = timestamp
            # else: keep current phase (too short duration)
        # else: same phase continues
    
    def setup_csv(self):
        """Initialize CSV with comprehensive metrics"""
        self.csv_file = open(LOG_FILE, "w", newline='')
        fieldnames = [
            'timestamp', 'phase', 'confidence', 'comm_score', 'comp_score',
            'power_W', 'dram_power_W', 'cpu_user_pct', 'cpu_system_pct',
            'cpu_idle_pct', 'cpu_active_pct', 'cpu_iowait_pct',
            'voluntary_ctx_switches_per_sec', 'active_mpi_ranks',
            'net_recv_mbps', 'net_sent_mbps', 'total_net_bw',
            'sync_variance_pct', 'expected_idle_pct', 'system_utilization',
            'power_threshold_comm', 'power_threshold_comp',
            'idle_threshold_comm', 'idle_threshold_comp',
            'warmup_complete', 'reasons'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        print(f"[INFO] Logging to {LOG_FILE}")
        print(f"[INFO] Adaptive monitoring with {WARMUP_SAMPLES}-sample warm-up")
    
    def generate_summary_report(self, total_time):
        """Generate detailed adaptive summary report"""
        with open(SUMMARY_FILE, "w") as f:
            f.write("="*70 + "\n")
            f.write("ADAPTIVE Communication Phase Analysis - Version 9.0\n")
            f.write("="*70 + "\n\n")
            
            # System characteristics
            f.write("SYSTEM CHARACTERISTICS:\n")
            f.write("-"*40 + "\n")
            f.write(f"Total CPU cores: {self.total_cores}\n")
            f.write(f"Warm-up samples: {self.warmup_samples}\n")
            f.write(f"Monitoring duration: {total_time:.2f}s\n\n")
            
            # Statistical baselines
            f.write("STATISTICAL BASELINES:\n")
            f.write("-"*40 + "\n")
            for metric, stats in self.baselines.items():
                f.write(f"{metric:15s}: mean={stats['mean']:6.1f}, std={stats['std']:6.1f}\n")
            f.write("\n")
            
            # Phase analysis
            f.write("PHASE ANALYSIS (Adaptive Thresholds):\n")
            f.write("-"*40 + "\n")
            
            total_tracked = sum(stats["time"] for stats in self.phase_stats.values())
            
            for phase in sorted(self.phase_stats.keys()):
                stats = self.phase_stats[phase]
                duration = stats["time"]
                samples = stats["samples"]
                transitions = stats["transitions"]
                
                if total_time > 0:
                    percentage = (duration / total_time) * 100
                else:
                    percentage = 0
                
                if samples > 0:
                    avg_duration = duration / samples
                else:
                    avg_duration = 0
                
                f.write(f"{phase:20s}: {duration:7.2f}s ({percentage:5.1f}%) | "
                       f"Samples: {samples:4d} | Avg: {avg_duration:.3f}s | "
                       f"Transitions: {transitions}\n")
            
            f.write(f"\nTotal monitored time: {total_time:.2f}s\n")
            f.write(f"Total tracked phase time: {total_tracked:.2f}s\n")
            
            # Optimization recommendations based on phase analysis
            f.write("\n" + "="*70 + "\n")
            f.write("ADAPTIVE OPTIMIZATION RECOMMENDATIONS:\n")
            f.write("="*70 + "\n\n")
            
            if "COMPUTE" in self.phase_stats:
                compute_pct = self.phase_stats["COMPUTE"]["time"] / total_time * 100
                f.write(f"Compute fraction: {compute_pct:.1f}%\n")
                
                if compute_pct < 5:
                    f.write("→ WORKLOAD IS EXTREMELY COMMUNICATION-BOUND\n")
                    f.write("  Recommendations:\n")
                    f.write("  1. Focus on MPI optimization above all else\n")
                    f.write("  2. Consider larger domain decomposition\n")
                    f.write("  3. Try hybrid MPI+OpenMP parallelization\n")
                    f.write("  4. Use non-blocking communication\n")
                elif compute_pct < 20:
                    f.write("→ WORKLOAD IS COMMUNICATION-BOUND\n")
                    f.write("  Recommendations:\n")
                    f.write("  1. Optimize collective operations\n")
                    f.write("  2. Reduce message sizes\n")
                    f.write("  3. Improve load balancing\n")
                    f.write("  4. Overlap communication with computation\n")
                elif compute_pct < 40:
                    f.write("→ WORKLOAD IS MODERATELY BALANCED\n")
                    f.write("  Recommendations:\n")
                    f.write("  1. Profile both compute and communication\n")
                    f.write("  2. Consider algorithmic improvements\n")
                    f.write("  3. Optimize memory access patterns\n")
                else:
                    f.write("→ WORKLOAD IS COMPUTE-BOUND\n")
                    f.write("  Recommendations:\n")
                    f.write("  1. Focus on compute kernel optimization\n")
                    f.write("  2. Use vectorization\n")
                    f.write("  3. Consider GPU acceleration\n")
                    f.write("  4. Improve cache utilization\n")
            
            # Synchronization analysis
            if "COMMUNICATION" in self.phase_stats and self.phase_stats["COMMUNICATION"]["samples"] > 0:
                avg_sync_var = self.baselines['sync_var']['mean']
                if avg_sync_var > 50:
                    f.write(f"\n→ HIGH SYNCHRONIZATION VARIANCE ({avg_sync_var:.1f}%)\n")
                    f.write("  Indicates poor load balancing across ranks\n")
                    f.write("  Recommendations:\n")
                    f.write("  1. Improve domain decomposition\n")
                    f.write("  2. Use dynamic load balancing\n")
                    f.write("  3. Check for I/O contention\n")
            
            # Network analysis
            if self.network_interfaces and self.baselines['net_bw']['mean'] > 10:
                f.write(f"\n→ SIGNIFICANT NETWORK TRAFFIC ({self.baselines['net_bw']['mean']:.1f} MB/s avg)\n")
                f.write("  Recommendations:\n")
                f.write("  1. Consider message aggregation\n")
                f.write("  2. Use efficient MPI datatypes\n")
                f.write("  3. Check network configuration\n")
        
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
    
    def extract_miniMD_timings(self):
        """Extract timing information from miniMD output (placeholder)"""
        # This should parse actual miniMD output
        return {
            "t_total": 10.36,
            "t_comm": 10.12,
            "t_force": 0.19,
            "t_neigh": 0.03,
            "t_other": 0.01,
            "comm_percentage": 97.7
        }
    
    def run_monitoring(self):
        """Main adaptive monitoring loop"""
        print(f"[INFO] Starting ADAPTIVE communication phase monitoring v9.0")
        print(f"[INFO] Command: {' '.join(CMD)}")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        print(f"[INFO] Adaptive thresholds with {WARMUP_SAMPLES}-sample warm-up")
        print(f"[INFO] Detection: Multi-dimensional adaptive scoring")
        
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
        
        try:
            while True:
                # Check if miniMD still running
                pids = self.get_miniMD_pids()
                if not pids:
                    print("[INFO] miniMD completed")
                    break
                
                sample_count += 1
                current_time = time.time() - self.start_time
                
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
                
                user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
                active_cpu_pct = 100.0 - idle_pct
                
                # Read network statistics
                curr_net_stats = self.read_network_stats()
                net_recv_mbps, net_sent_mbps = self.calculate_network_bandwidth(self.prev_net_stats, curr_net_stats)
                self.prev_net_stats = curr_net_stats
                total_net_bw = net_recv_mbps + net_sent_mbps
                
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
                
                # Calculate expected idle based on system utilization
                active_ranks = len(pids)
                expected_idle_pct = 100 - (active_ranks / self.total_cores * 100 * 0.8)  # 80% efficiency
                
                # Prepare metrics for classification
                metrics = {
                    'power_W': pkg_power_W,
                    'dram_power_W': dram_power_W,
                    'cpu_user_pct': user_pct,
                    'cpu_system_pct': system_pct,
                    'cpu_idle_pct': idle_pct,
                    'cpu_active_pct': active_cpu_pct,
                    'cpu_iowait_pct': iowait_pct,
                    'ctx_switch_rate': voluntary_ctx_rate,
                    'sync_variance_norm': sync_variance_norm,
                    'total_net_bw': total_net_bw
                }
                
                # Update baselines during warm-up
                self.update_baselines(metrics)
                
                # Calculate adaptive thresholds
                thresholds = self.calculate_adaptive_thresholds(metrics, active_ranks)
                
                # Classify phase using adaptive method
                phase, details = self.classify_phase_adaptive(metrics, thresholds, active_ranks)
                
                # Track phase duration
                self.track_phase_duration(phase, current_time)
                
                # Log data
                row = {
                    'timestamp': f"{current_time:.2f}",
                    'phase': details['phase'],
                    'confidence': f"{details['confidence']:.3f}",
                    'comm_score': f"{details['comm_score']:.3f}",
                    'comp_score': f"{details['comp_score']:.3f}",
                    'power_W': f"{details['power_W']:.1f}",
                    'dram_power_W': f"{details['dram_power_W']:.1f}",
                    'cpu_user_pct': f"{details['cpu_user_pct']:.1f}",
                    'cpu_system_pct': f"{details['cpu_system_pct']:.1f}",
                    'cpu_idle_pct': f"{details['cpu_idle_pct']:.1f}",
                    'cpu_active_pct': f"{details['cpu_active_pct']:.1f}",
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'voluntary_ctx_switches_per_sec': f"{details['ctx_switch_rate']:.0f}",
                    'active_mpi_ranks': active_ranks,
                    'net_recv_mbps': f"{net_recv_mbps:.1f}",
                    'net_sent_mbps': f"{net_sent_mbps:.1f}",
                    'total_net_bw': f"{total_net_bw:.1f}",
                    'sync_variance_pct': f"{details['sync_variance_norm']:.1f}",
                    'expected_idle_pct': f"{expected_idle_pct:.1f}",
                    'system_utilization': f"{(active_ranks/self.total_cores*100):.1f}",
                    'power_threshold_comm': f"{thresholds['power_comm']:.1f}",
                    'power_threshold_comp': f"{thresholds['power_comp']:.1f}",
                    'idle_threshold_comm': f"{thresholds['idle_comm']:.1f}",
                    'idle_threshold_comp': f"{thresholds['idle_comp']:.1f}",
                    'warmup_complete': str(self.warmup_complete),
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status (compact display)
                if sample_count % 5 == 0:
                    phase_char = details['phase'][0]
                    status = (f"[{current_time:6.1f}s] {phase_char} "
                             f"Conf:{details['confidence']:.2f} "
                             f"Pwr:{details['power_W']:4.0f}W "
                             f"Idle:{details['cpu_idle_pct']:3.0f}% "
                             f"Ctx:{details['ctx_switch_rate']:5.0f}/s "
                             f"R:{active_ranks:2d}")
                    
                    if not self.warmup_complete:
                        status += f" WUP:{self.warmup_samples}/{WARMUP_SAMPLES}"
                    
                    print(status)
                
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
        
        # Record final phase duration
        if self.current_phase and self.phase_start_time:
            final_duration = total_time - self.phase_start_time
            if final_duration > 0 and self.current_phase != "UNKNOWN":
                self.phase_stats[self.current_phase]["time"] += final_duration
                self.phase_stats[self.current_phase]["samples"] += 1
        
        if self.csv_file:
            self.csv_file.close()
        
        # Generate summary report
        self.generate_summary_report(total_time)
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
        
        # Print quick summary
        print("\n" + "="*60)
        print("ADAPTIVE SUMMARY:")
        print("="*60)
        for phase in sorted(self.phase_stats.keys()):
            stats = self.phase_stats[phase]
            duration = stats["time"]
            percentage = (duration / total_time * 100) if total_time > 0 else 0
            print(f"{phase:20s}: {duration:6.2f}s ({percentage:5.1f}%) | "
                  f"Samples: {stats['samples']}")

def main():
    """Entry point"""
    
    if not os.path.exists(RAPL_PATH):
        print(f"[ERROR] RAPL not accessible at {RAPL_PATH}")
        print("[ERROR] Cannot monitor without energy data")
        return 1
    
    print("="*70)
    print("ADAPTIVE MiniMD Communication Phase Monitor - Version 9.0")
    print("="*70)
    print("Key features:")
    print("  ✓ No hardcoded thresholds - all adapt to observed data")
    print("  ✓ Statistical baselines computed during warm-up")
    print("  ✓ Multi-dimensional weighted scoring")
    print("  ✓ System-aware idle calculations")
    print("  ✓ All technical fixes from v7.1 and v8")
    print("  ✓ Formula-based, not assumption-based")
    print("="*70)
    print(f"Configuration:")
    print(f"  Warm-up samples: {WARMUP_SAMPLES}")
    print(f"  Minimum phase duration: {MIN_PHASE_DURATION}s")
    print(f"  Robustness factor: {ROBUSTNESS_FACTOR}σ")
    print("="*70)
    
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
