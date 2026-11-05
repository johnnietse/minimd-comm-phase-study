#!/usr/bin/env python3
"""
Enhanced Communication Phase Monitor for miniMD on Frontenac
Version 7.0 - Added network monitoring, synchronization detection, and improved analysis
Uses /proc-based metrics + RAPL for cluster-compatible monitoring
Focuses on detecting MPI communication patterns without requiring perf permissions
"""

import subprocess
import time
import csv
import os
import sys
import math
import statistics
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime

# ============ CONFIGURATION ============
CMD = ["mpirun", "--oversubscribe", "-np", "4", 
       "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = f"comm_phase_monitoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
SUMMARY_FILE = f"comm_phase_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
SAMPLE_INTERVAL = 0.2  # 200ms for better temporal resolution
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_DRAM_PATH = "/sys/class/powercap/intel-rapl:0:0/energy_uj"  # DRAM energy

# Communication phase thresholds (calibrated for miniMD MPI patterns)
POWER_COMM_LOW = 150.0      # Communication phases often <200W (waiting)
POWER_COMPUTE_HIGH = 250.0  # Compute phases >250W
CPU_IDLE_THRESHOLD = 60.0   # >60% idle = likely waiting in MPI
VOLUNTARY_CTX_HIGH = 50     # High voluntary switches = MPI blocking calls
SYNC_VARIANCE_HIGH = 100.0  # High CPU time variance = poor synchronization
NETWORK_BW_THRESHOLD = 1000000  # 1 MB/s threshold for network activity

# Network interface to monitor (common HPC interfaces)
NETWORK_INTERFACES = ["ib0", "ib1", "eth0", "eth1", "eno1", "ens1"]

class CommPhaseMonitor:
    def __init__(self):
        self.proc = None
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.prev_dram_energy = None
        self.prev_proc_stats = {}
        self.prev_net_stats = {}
        self.prev_ctx_switches = {}
        self.has_dram_rapl = os.path.exists(RAPL_DRAM_PATH)
        
        # Phase tracking for duration analysis
        self.phase_start_time = None
        self.current_phase = None
        self.phase_durations = defaultdict(float)
        self.phase_samples = defaultdict(int)
        
        # For variance calculation
        self.per_process_cpu_times = deque(maxlen=100)  # Keep last 100 samples
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
                return [int(pid) for pid in result.stdout.strip().split()]
        except Exception as e:
            pass
        return []
    
    def safe_delta(self, current, previous, counter_name=""):
        """
        Safe delta calculation with counter wrap-around handling
        RAPL counters are 32-bit for some domains, 64-bit for others
        """
        if current >= previous:
            return current - previous
        
        # Counter wrap-around detected
        # Determine max value based on counter type
        if "energy_uj" in counter_name or "rapl" in counter_name.lower():
            # RAPL energy counters: typically 32-bit for package, 64-bit for DRAM?
            # From Intel docs: most are 32-bit, but we'll use 2^32 as safe max
            max_value = 2**32
        else:
            # Context switches and other counters
            max_value = 2**64
        
        delta = (max_value - previous) + current
        print(f"[DEBUG] Counter wrap-around detected for {counter_name}: {previous} -> {current}, delta={delta}")
        return delta
    
    def read_rapl_energy(self, path, counter_name=""):
        """Read RAPL energy counter (microjoules) with error handling"""
        try:
            with open(path, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            print(f"[WARN] Failed to read RAPL at {path}: {e}")
            return 0
    
    def read_network_stats(self):
        """
        Read network statistics from /proc/net/dev
        Returns dict with {interface: (bytes_recv, bytes_sent)}
        """
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
        """
        Calculate network bandwidth in bytes per second
        Returns: (total_recv_bw, total_sent_bw) in MB/s
        """
        total_recv_delta = 0
        total_sent_delta = 0
        
        for iface in self.network_interfaces:
            if iface in prev_stats and iface in curr_stats:
                prev_recv, prev_sent = prev_stats[iface]
                curr_recv, curr_sent = curr_stats[iface]
                
                # Use safe delta for network counters
                recv_delta = self.safe_delta(curr_recv, prev_recv, f"net_{iface}_recv")
                sent_delta = self.safe_delta(curr_sent, prev_sent, f"net_{iface}_sent")
                
                total_recv_delta += recv_delta
                total_sent_delta += sent_delta
        
        # Convert to MB/s (bytes to megabytes)
        recv_mbps = total_recv_delta / SAMPLE_INTERVAL / (1024 * 1024)
        sent_mbps = total_sent_delta / SAMPLE_INTERVAL / (1024 * 1024)
        
        return recv_mbps, sent_mbps
    
    def get_per_process_cpu_usage(self, pids):
        """
        Get CPU usage times for each process to measure synchronization
        Returns: list of total CPU times (user + system) for each process
        """
        cpu_times = []
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", "r") as f:
                    stat_data = f.read().split()
                    # Position 13: utime, 14: stime (in clock ticks)
                    utime = int(stat_data[13])
                    stime = int(stat_data[14])
                    cpu_times.append(utime + stime)
            except (FileNotFoundError, ValueError, IndexError) as e:
                continue
        return cpu_times
    
    def calculate_sync_variance(self, cpu_times):
        """
        Calculate synchronization variance across processes
        High variance = poor synchronization
        Returns variance and normalized variance (0-100)
        """
        if len(cpu_times) < 2:
            return 0.0, 0.0
        
        try:
            variance = statistics.variance(cpu_times)
            mean = statistics.mean(cpu_times)
            # Normalize variance relative to mean
            norm_variance = (variance / mean * 100) if mean > 0 else 0
            return variance, norm_variance
        except statistics.StatisticsError:
            return 0.0, 0.0
    
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
        Aggregate statistics from /proc/[pid]/status for all miniMD processes
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
    
    def classify_comm_phase(self, power_W, dram_power_W, cpu_metrics, 
                           ctx_switch_rate, pids_count, network_bw,
                           sync_variance_norm):
        """
        Enhanced phase classification with network and synchronization metrics
        
        MPI Communication phases in miniMD are characterized by:
        1. LOW power consumption (CPUs waiting, not computing)
        2. HIGH CPU idle time (blocked in MPI calls)
        3. HIGH voluntary context switches (MPI_Wait, MPI_Recv blocking)
        4. LOW DRAM power (not moving data through memory)
        5. MULTIPLE active processes (MPI ranks coordinating)
        6. NETWORK activity (if using network MPI)
        7. LOW synchronization variance (ranks should be waiting together)
        """
        
        user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
        net_recv_mbps, net_sent_mbps = network_bw
        
        phase = "UNKNOWN"
        confidence = 0.0
        reasons = []
        
        # Calculate active CPU (inverse of idle)
        active_cpu = 100.0 - idle_pct
        total_net_bw = net_recv_mbps + net_sent_mbps
        
        # Indicator 1: Low power = waiting in MPI (not computing)
        if power_W < POWER_COMM_LOW and power_W > 50:  # Not idle, but not computing
            confidence += 0.30
            reasons.append(f"low_power({power_W:.0f}W)")
        
        # Indicator 2: High idle time = CPUs blocked in MPI
        if idle_pct > CPU_IDLE_THRESHOLD:
            confidence += 0.25
            reasons.append(f"high_idle({idle_pct:.0f}%)")
        
        # Indicator 3: High voluntary context switches = MPI blocking calls
        if ctx_switch_rate > VOLUNTARY_CTX_HIGH:
            confidence += 0.20
            reasons.append(f"mpi_blocking({ctx_switch_rate:.0f}/s)")
        
        # Indicator 4: Multiple processes active (MPI coordination)
        if pids_count >= 8:  # At least 8 ranks active
            confidence += 0.10
            reasons.append(f"multi_rank({pids_count})")
        
        # Indicator 5: Network activity (if available)
        if total_net_bw > NETWORK_BW_THRESHOLD / (1024*1024):  # Convert threshold to MB/s
            confidence += 0.10
            reasons.append(f"net_bw({total_net_bw:.1f}MB/s)")
        
        # Indicator 6: Low synchronization variance (ranks waiting together)
        if sync_variance_norm < 10:  # Low variance = good synchronization
            confidence += 0.05
            reasons.append(f"low_sync_var({sync_variance_norm:.1f}%)")
        
        # Phase classification
        if confidence >= 0.50:
            phase = "COMMUNICATION"
        elif power_W > POWER_COMPUTE_HIGH and active_cpu > 70:
            phase = "COMPUTE"
            reasons.append(f"high_power({power_W:.0f}W),high_cpu({active_cpu:.0f}%)")
        elif iowait_pct > 10:
            phase = "IO_WAIT"
            reasons.append(f"high_iowait({iowait_pct:.1f}%)")
        elif idle_pct > 90:
            phase = "IDLE"
            reasons.append(f"high_idle({idle_pct:.1f}%)")
        elif total_net_bw > 50:  # High network bandwidth
            phase = "NETWORK_COMM"
            reasons.append(f"high_net_bw({total_net_bw:.1f}MB/s)")
        else:
            phase = "MIXED"
        
        # Adjust confidence for non-communication phases
        if phase != "COMMUNICATION":
            confidence = max(0.0, confidence - 0.3)  # Lower confidence for non-comm phases
        
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
            'net_recv_mbps': net_recv_mbps,
            'net_sent_mbps': net_sent_mbps,
            'sync_variance': sync_variance_norm,
            'reasons': ','.join(reasons) if reasons else 'none'
        }
        
        return phase, details
    
    def track_phase_duration(self, new_phase, timestamp):
        """Track duration of each phase for summary statistics"""
        if self.current_phase is None:
            self.current_phase = new_phase
            self.phase_start_time = timestamp
        elif self.current_phase != new_phase:
            # Phase changed, record duration of previous phase
            if self.phase_start_time is not None:
                duration = timestamp - self.phase_start_time
                self.phase_durations[self.current_phase] += duration
                self.phase_samples[self.current_phase] += 1
            
            # Start new phase
            self.current_phase = new_phase
            self.phase_start_time = timestamp
        # else: same phase continues
    
    def setup_csv(self):
        """Initialize CSV with enhanced metrics"""
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
            'net_recv_mbps',
            'net_sent_mbps',
            'sync_variance_pct',
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
        if self.network_interfaces:
            print(f"[INFO] Monitoring network interfaces: {self.network_interfaces}")
        else:
            print("[INFO] No network interfaces available for monitoring")
    
    def generate_summary_report(self, total_time):
        """Generate detailed summary report"""
        with open(SUMMARY_FILE, "w") as f:
            f.write("="*60 + "\n")
            f.write("MiniMD Communication Phase Analysis Summary\n")
            f.write("="*60 + "\n\n")
            
            f.write("PHASE DURATION ANALYSIS:\n")
            f.write("-"*40 + "\n")
            total_tracked = sum(self.phase_durations.values())
            
            for phase in sorted(self.phase_durations.keys()):
                duration = self.phase_durations[phase]
                percentage = (duration / total_time * 100) if total_time > 0 else 0
                samples = self.phase_samples.get(phase, 0)
                avg_duration = duration / samples if samples > 0 else 0
                
                f.write(f"{phase:20s}: {duration:7.2f}s ({percentage:5.1f}%) | "
                       f"Samples: {samples:4d} | Avg: {avg_duration:.2f}s\n")
            
            f.write(f"\nTotal monitored time: {total_time:.2f}s\n")
            f.write(f"Total tracked phase time: {total_tracked:.2f}s\n")
            
            # Calculate communication efficiency
            if "COMMUNICATION" in self.phase_durations:
                comm_time = self.phase_durations["COMMUNICATION"]
                comm_efficiency = (comm_time / total_time * 100) if total_time > 0 else 0
                f.write(f"\nCOMMUNICATION ANALYSIS:\n")
                f.write("-"*40 + "\n")
                f.write(f"Time in communication: {comm_time:.2f}s ({comm_efficiency:.1f}%)\n")
                f.write(f"Expected (from miniMD): ~97.7%\n")
                
                if comm_efficiency < 95:
                    f.write(f"NOTE: Lower than expected communication time\n")
                    f.write(f"      Could indicate: Better overlap or measurement error\n")
                elif comm_efficiency > 99:
                    f.write(f"NOTE: Very high communication time\n")
                    f.write(f"      Could indicate: MPI synchronization issues\n")
            
            f.write("\n" + "="*60 + "\n")
            f.write("RECOMMENDATIONS FOR OPTIMIZATION:\n")
            f.write("="*60 + "\n\n")
            
            if "COMPUTE" in self.phase_durations and self.phase_durations["COMPUTE"] > 0:
                f.write("1. Compute phases detected - consider:\n")
                f.write("   - Vectorization of force calculations\n")
                f.write("   - Thread parallelism within MPI ranks\n")
                f.write("   - Algorithmic improvements\n")
            
            if "NETWORK_COMM" in self.phase_durations:
                f.write("2. Network communication detected - consider:\n")
                f.write("   - Using non-blocking MPI calls\n")
                f.write("   - Overlap computation with communication\n")
                f.write("   - Message aggregation\n")
            
            f.write("3. General MPI optimizations:\n")
            f.write("   - Reduce message sizes (ghost cell optimization)\n")
            f.write("   - Improve load balancing\n")
            f.write("   - Use MPI_Iallreduce for global operations\n")
        
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
    
    def run_monitoring(self):
        """Main monitoring loop with enhanced metrics"""
        print(f"[INFO] Starting enhanced communication phase monitoring")
        print(f"[INFO] Command: {' '.join(CMD)}")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        print(f"[INFO] Detection: Power + CPU idle + context switches + network + sync")
        
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
        
        # Initialize CPU and network stats
        self.prev_proc_stats = self.read_proc_stat()
        self.prev_net_stats = self.read_network_stats()
        self.prev_ctx_switches = (0, 0)
        
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
                
                # Read current energy with safe delta
                energy_now = self.read_rapl_energy(RAPL_PATH, "pkg_energy")
                delta_pkg_energy = self.safe_delta(energy_now, self.prev_energy, "pkg_energy_uj") / 1e6  # µJ to J
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
                
                # Read context switches
                curr_ctx_switches = self.get_proc_stats_for_pids(pids)
                voluntary_ctx_delta = self.safe_delta(
                    curr_ctx_switches[0], self.prev_ctx_switches[0], "voluntary_ctx"
                )
                voluntary_ctx_rate = voluntary_ctx_delta / SAMPLE_INTERVAL
                self.prev_ctx_switches = curr_ctx_switches
                
                # Get per-process CPU times for synchronization analysis
                per_process_cpu = self.get_per_process_cpu_usage(pids)
                sync_variance, sync_variance_norm = self.calculate_sync_variance(per_process_cpu)
                
                # Classify phase
                phase, details = self.classify_comm_phase(
                    pkg_power_W, dram_power_W, cpu_metrics, 
                    voluntary_ctx_rate, len(pids),
                    (net_recv_mbps, net_sent_mbps),
                    sync_variance_norm
                )
                
                # Track phase duration
                self.track_phase_duration(phase, current_time)
                
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
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'voluntary_ctx_switches_per_sec': f"{details['voluntary_ctx_switches_per_sec']:.0f}",
                    'active_mpi_ranks': details['active_mpi_ranks'],
                    'net_recv_mbps': f"{details['net_recv_mbps']:.1f}",
                    'net_sent_mbps': f"{details['net_sent_mbps']:.1f}",
                    'sync_variance_pct': f"{details['sync_variance']:.1f}",
                    'pkg_energy_J': f"{delta_pkg_energy:.4f}",
                    'dram_energy_J': f"{delta_dram_energy:.4f}",
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status (enhanced)
                if sample_count % 5 == 0:
                    net_info = f"Net:{details['net_recv_mbps']:.0f}+{details['net_sent_mbps']:.0f}MB/s" if self.network_interfaces else ""
                    sync_info = f"Sync:{details['sync_variance']:.1f}%" if len(pids) > 1 else ""
                    print(f"[{current_time:6.1f}s] {details['phase']:15} "
                          f"Conf:{details['confidence']:.2f} "
                          f"Pwr:{details['power_W']:4.0f}W "
                          f"Idle:{details['cpu_idle_pct']:3.0f}% "
                          f"Ctx:{details['voluntary_ctx_switches_per_sec']:5.0f}/s "
                          f"Ranks:{details['active_mpi_ranks']:2d} "
                          f"{net_info} {sync_info}")
                
        except KeyboardInterrupt:
            print("\n[INFO] Monitoring interrupted by user")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources and generate summary"""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        # Record final phase duration
        if self.current_phase and self.phase_start_time:
            final_duration = total_time - self.phase_start_time
            self.phase_durations[self.current_phase] += final_duration
            self.phase_samples[self.current_phase] += 1
        
        if self.csv_file:
            self.csv_file.close()
        
        # Generate summary report
        self.generate_summary_report(total_time)
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")

def main():
    """Entry point"""
    
    # Validate environment
    if not os.path.exists(RAPL_PATH):
        print(f"[ERROR] RAPL not accessible at {RAPL_PATH}")
        print("[ERROR] Cannot monitor without energy data")
        return 1
    
    print("="*60)
    print("Enhanced MiniMD Communication Phase Monitor - Version 7.0")
    print("="*60)
    print("Detection methods:")
    print("  - Power monitoring (RAPL)")
    print("  - CPU utilization (/proc/stat)")
    print("  - Context switches (/proc/[pid]/status)")
    print("  - Network bandwidth (/proc/net/dev)")
    print("  - Synchronization variance (per-process CPU times)")
    print("="*60)
    print(f"Thresholds:")
    print(f"  Power < {POWER_COMM_LOW}W, Idle > {CPU_IDLE_THRESHOLD}%, CtxSw > {VOLUNTARY_CTX_HIGH}/s")
    print(f"  Sync variance < {SYNC_VARIANCE_HIGH}%, Network > {NETWORK_BW_THRESHOLD/1e6:.1f} MB/s")
    print("="*60)
    
    monitor = CommPhaseMonitor()
    
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
