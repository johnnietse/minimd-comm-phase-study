#!/usr/bin/env python3
"""
INTELLIGENT Communication Phase Monitor for miniMD - Version 15.0
Enhanced with optimization strategies for communication-bound workloads
Key improvements:
1. Multiple optimization profiles (tuned, hybrid, network_optimized, etc.)
2. Process affinity and binding strategies
3. MPI parameter tuning based on rank count
4. Environment variable optimization
5. Hybrid MPI+OpenMP support
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
import signal
import shlex
from pathlib import Path
from collections import defaultdict, deque, Counter
from datetime import datetime

# ============ CONFIGURATION ============
# Default command - will be modified based on optimization profile
DEFAULT_CMD = ["mpirun", "--oversubscribe", "-np", "32", "./miniMD_openmpi", "-i", "in.lj.miniMD"]

# Base filename
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE = f"comm_phase_monitoring_{timestamp}.csv"
SUMMARY_FILE = f"comm_phase_summary_{timestamp}.txt"
SCALING_FILE = f"scaling_analysis_{timestamp}.json"
OPTIMIZATION_FILE = f"optimization_report_{timestamp}.json"

SAMPLE_INTERVAL = 0.2
MAX_CTX_RATE = 1e6

# Network interface to monitor
NETWORK_INTERFACES = ["ib0", "ib1", "eth0", "eth1", "eno1", "ens1", "bond0"]

# Empirical scaling data from your miniMD runs (communication percentages)
EMPIRICAL_SCALING_DATA = {
    2: {"comm_pct": 23.7, "compute_pct": 62.5, "force_pct": 62.5, "neigh_pct": 12.6},
    4: {"comm_pct": 40.2, "compute_pct": 47.5, "force_pct": 47.5, "neigh_pct": 11.7},
    8: {"comm_pct": 75.5, "compute_pct": 14.9, "force_pct": 14.9, "neigh_pct": 9.3},
    16: {"comm_pct": 87.5, "compute_pct": 4.6, "force_pct": 4.6, "neigh_pct": 7.7},
    32: {"comm_pct": 96.9, "compute_pct": 1.9, "force_pct": 1.9, "neigh_pct": 0.3},
    64: {"comm_pct": 97.8, "compute_pct": 0.8, "force_pct": 0.8, "neigh_pct": 0.2}
}

# Unreliable metrics at low MPI ranks (should be avoided or normalized)
UNRELIABLE_LOW_RANK_METRICS = ["node_idle_pct", "node_active_pct", "raw_power_W"]

# Optimization profiles for frnt115 (gpu-rtx4000 node)
OPTIMIZATION_PROFILES = {
    "default": {
        "description": "Default MPI settings (no optimizations)",
        "mpi_args": ["--oversubscribe"],
        "env_vars": {},
        "recommended_for": "Baseline comparison"
    },
    "tuned": {
        "description": "Tuned MPI parameters for Ethernet communication",
        "mpi_args": [
            "--oversubscribe",
            "--mca", "btl", "^openib",  # Disable InfiniBand
            "--mca", "btl_tcp_if_include", "eno1,eno2",
            "--mca", "btl_tcp_rcv_buf", "16777216",
            "--mca", "btl_tcp_snd_buf", "16777216",
            "--mca", "pml", "ob1",
            "--mca", "coll_tuned_use_dynamic_rules", "1"
        ],
        "env_vars": {
            "OMPI_MCA_btl": "self,sm,tcp",
            "OMPI_MCA_pml": "ob1",
            "OMPI_MCA_coll_tuned_use_dynamic_rules": "1"
        },
        "recommended_for": "Medium to high rank counts (>16 ranks)"
    },
    "affinity_core": {
        "description": "Core affinity binding",
        "mpi_args": [
            "--oversubscribe",
            "--bind-to", "core",
            "--map-by", "core",
            "--report-bindings"
        ],
        "env_vars": {},
        "recommended_for": "All rank counts, especially compute-bound"
    },
    "affinity_socket": {
        "description": "Socket affinity binding (NUMA-aware)",
        "mpi_args": [
            "--oversubscribe",
            "--bind-to", "socket",
            "--map-by", "socket"
        ],
        "env_vars": {},
        "recommended_for": "NUMA systems, large memory footprints"
    },
    "hybrid_4threads": {
        "description": "Hybrid MPI+OpenMP with 4 threads per rank",
        "mpi_args": [
            "--oversubscribe",
            "--bind-to", "socket",
            "--map-by", "socket:PE=4"
        ],
        "env_vars": {
            "OMP_NUM_THREADS": "4",
            "OMP_PROC_BIND": "true",
            "OMP_PLACES": "cores"
        },
        "recommended_for": "Communication-bound workloads, reduce MPI ranks"
    },
    "hybrid_8threads": {
        "description": "Hybrid MPI+OpenMP with 8 threads per rank",
        "mpi_args": [
            "--oversubscribe",
            "--bind-to", "socket",
            "--map-by", "socket:PE=8"
        ],
        "env_vars": {
            "OMP_NUM_THREADS": "8",
            "OMP_PROC_BIND": "true",
            "OMP_PLACES": "cores"
        },
        "recommended_for": "Very communication-bound, low MPI rank count"
    },
    "network_optimized": {
        "description": "Network-optimized for Ethernet (eno1, eno2)",
        "mpi_args": [
            "--oversubscribe",
            "--mca", "btl", "tcp,self",
            "--mca", "btl_tcp_if_include", "eno1,eno2",
            "--mca", "btl_tcp_port_min_v4", "60000",
            "--mca", "btl_tcp_port_max_v4", "61000"
        ],
        "env_vars": {
            "OMPI_MCA_btl": "tcp,self",
            "OMPI_MCA_btl_tcp_if_include": "eno1,eno2"
        },
        "recommended_for": "High network traffic, many small messages"
    },
    "memory_optimized": {
        "description": "Memory and buffer optimizations",
        "mpi_args": [
            "--oversubscribe",
            "--mca", "pml_ob1_rdma_buffer_size", "16777216",
            "--mca", "pml_ob1_send_buffer_size", "16777216",
            "--mca", "pml_ob1_recv_buffer_size", "16777216"
        ],
        "env_vars": {
            "OMPI_MCA_pml_ob1_rdma_buffer_size": "16777216",
            "OMPI_MCA_pml_ob1_send_buffer_size": "16777216",
            "OMPI_MCA_pml_ob1_recv_buffer_size": "16777216"
        },
        "recommended_for": "Large messages, memory-bound applications"
    },
    "coll_optimized": {
        "description": "Collective operation optimizations",
        "mpi_args": [
            "--oversubscribe",
            "--mca", "coll_base_verbose", "1",
            "--mca", "coll_tuned_use_dynamic_rules", "1",
            "--mca", "coll_tuned_bcast_algorithm", "1",
            "--mca", "coll_tuned_allreduce_algorithm", "1"
        ],
        "env_vars": {
            "OMPI_MCA_coll_tuned_use_dynamic_rules": "1",
            "OMPI_MCA_coll_tuned_bcast_algorithm": "1"
        },
        "recommended_for": "Heavy use of collectives (broadcast, reduce)"
    }
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

def validate_environment():
    """Check if all required tools and permissions are available"""
    errors = []
    warnings = []
    
    # Check for RAPL
    if not RAPL_PATH:
        errors.append("RAPL power monitoring not available (requires root or appropriate permissions)")
    else:
        # Check if we can read RAPL
        try:
            with open(RAPL_PATH, "r") as f:
                _ = f.read()
        except PermissionError:
            errors.append(f"Permission denied for RAPL at {RAPL_PATH}. Try running with sudo.")
        except Exception as e:
            errors.append(f"Cannot read RAPL: {e}")
    
    # Check for required commands
    required_cmds = ['mpirun', 'pgrep', 'ps']
    for cmd in required_cmds:
        try:
            subprocess.run([cmd, '--version'], capture_output=True, timeout=2)
        except FileNotFoundError:
            warnings.append(f"Command '{cmd}' not found in PATH")
        except subprocess.TimeoutExpired:
            warnings.append(f"Command '{cmd}' timed out")
        except Exception:
            warnings.append(f"Command '{cmd}' check failed")
    
    # Check Python version
    if sys.version_info < (3, 6):
        errors.append("Python 3.6 or higher required")
    
    if errors:
        print("CRITICAL ERRORS:")
        for error in errors:
            print(f"  - {error}")
        print("\nPlease fix these issues before running the monitor.")
        return False
    
    if warnings:
        print("WARNINGS:")
        for warning in warnings:
            print(f"  - {warning}")
        print("\nContinuing with reduced functionality...")
    
    return True

class OptimizationManager:
    """Manages optimization profiles and builds appropriate commands"""
    
    def __init__(self, profile="default", ranks=32, executable="./miniMD_openmpi", 
                 input_file="in.lj.miniMD", extra_args=None):
        self.profile = profile
        self.ranks = ranks
        self.executable = executable
        self.input_file = input_file
        self.extra_args = extra_args or []
        self.results = {}
        
    def build_command(self):
        """Build command based on optimization profile"""
        if self.profile not in OPTIMIZATION_PROFILES:
            print(f"[WARN] Profile '{self.profile}' not found, using 'default'")
            self.profile = "default"
        
        profile_info = OPTIMIZATION_PROFILES[self.profile]
        
        # Start with mpirun and rank count
        cmd_parts = ["mpirun", "-np", str(self.ranks)]
        
        # Add MPI arguments from profile
        cmd_parts.extend(profile_info["mpi_args"])
        
        # Add executable and input file
        cmd_parts.extend([self.executable, "-i", self.input_file])
        
        # Add any extra arguments
        cmd_parts.extend(self.extra_args)
        
        # Set environment variables
        env_vars = profile_info["env_vars"].copy()
        
        # Add rank-specific environment variables
        if self.ranks <= 4:
            env_vars.update({
                "OMPI_MCA_orte_process_binding": "core",
                "OMPI_MCA_orte_process_mapping": "core"
            })
        elif self.ranks <= 16:
            env_vars.update({
                "OMPI_MCA_btl_tcp_if_include": "eno1,eno2",
            })
        else:  # High ranks
            env_vars.update({
                "OMPI_MCA_btl_tcp_if_include": "eno1,eno2",
                "OMPI_MCA_btl_tcp_eager_limit": "524288",
            })
        
        return cmd_parts, env_vars
    
    def recommend_profile(self, rank_count, previous_results=None):
        """Recommend optimization profile based on rank count and previous results"""
        recommendations = []
        
        if rank_count <= 4:
            # Low ranks: compute-bound, use hybrid to reduce communication
            recommendations.append(("hybrid_8threads", 
                "Low MPI ranks benefit from hybrid MPI+OpenMP to reduce communication overhead"))
            recommendations.append(("affinity_core",
                "Core affinity improves cache locality for compute-bound workloads"))
        elif rank_count <= 16:
            # Medium ranks: balanced
            recommendations.append(("tuned",
                "Tuned MPI parameters optimize Ethernet communication"))
            recommendations.append(("hybrid_4threads",
                "Hybrid approach balances communication and computation"))
        else:
            # High ranks: communication-bound
            recommendations.append(("network_optimized",
                "Network optimization crucial for communication-bound workloads"))
            recommendations.append(("memory_optimized",
                "Large buffers help with many small messages"))
        
        return recommendations
    
    def record_results(self, phase_data, performance_data):
        """Record results for this optimization profile"""
        self.results = {
            "profile": self.profile,
            "ranks": self.ranks,
            "phase_distribution": phase_data,
            "performance": performance_data,
            "timestamp": datetime.now().isoformat()
        }

class IntelligentCommPhaseMonitor:
    def __init__(self, expected_ranks=None, optimization_profile="default", 
                 executable="./miniMD_openmpi", input_file="in.lj.miniMD"):
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.prev_dram_energy = None
        self.prev_proc_stats = {}
        self.prev_net_stats = {}
        self.prev_ctx_switches = {}
        self.prev_per_process_cpu = {}
        self.actual_mpi_ranks = None
        
        self.has_dram_rapl = RAPL_DRAM_PATH is not None and os.path.exists(RAPL_DRAM_PATH)
        
        # Enhanced phase tracking with learning
        self.current_phase = "INIT"
        self.phase_start_time = None
        self.phase_stats = defaultdict(lambda: {"time": 0.0, "samples": 0, "transitions": 0})
        self.phase_sequence = []
        
        # Runtime learning system
        self.power_distribution = []
        self.idle_distribution = []
        self.ctx_distribution = []
        self.sync_distribution = []
        
        # Track patterns for phase detection
        self.power_pattern = deque(maxlen=20)
        self.cpu_pattern = deque(maxlen=20)
        self.ctx_pattern = deque(maxlen=20)
        
        # Expected behavior based on scaling data
        self.expected_ranks = expected_ranks or 32
        self.actual_detected_ranks = None
        
        # Use empirical data for calibration
        if self.expected_ranks in EMPIRICAL_SCALING_DATA:
            self.expected_comm_pct = EMPIRICAL_SCALING_DATA[self.expected_ranks]["comm_pct"]
            self.expected_compute_pct = EMPIRICAL_SCALING_DATA[self.expected_ranks]["compute_pct"]
        else:
            self.estimate_expected_behavior()
        
        # Initialize optimization manager
        self.optimization_manager = OptimizationManager(
            profile=optimization_profile,
            ranks=self.expected_ranks,
            executable=executable,
            input_file=input_file
        )
        
        # Build command with optimizations
        self.cmd, self.env_vars = self.optimization_manager.build_command()
        
        # Adaptive thresholds
        self.initialize_rank_aware_thresholds()
        
        # Performance metrics
        self.compute_bursts = []
        self.communication_intervals = []
        self.phase_durations_history = defaultdict(list)
        
        # Pattern detection
        self.phase_patterns = []
        self.last_phase_change_time = None
        self.phase_change_count = 0
        
        self.network_interfaces = self.detect_network_interfaces()
        
        print(f"[INFO] Using optimization profile: {optimization_profile}")
        print(f"[INFO] Profile description: {OPTIMIZATION_PROFILES[optimization_profile]['description']}")
        print(f"[INFO] Recommended for: {OPTIMIZATION_PROFILES[optimization_profile]['recommended_for']}")
        print(f"[INFO] Command: {' '.join(self.cmd)}")
        if self.env_vars:
            print(f"[INFO] Environment variables: {self.env_vars}")
        print(f"[INFO] Detected network interfaces: {self.network_interfaces}")
        if self.has_dram_rapl:
            print("[INFO] DRAM RAPL available - tracking memory power")
    
    def initialize_rank_aware_thresholds(self):
        """Initialize thresholds based on expected rank count"""
        if self.expected_ranks <= 4:
            self.dynamic_thresholds = {
                'power_per_rank_threshold': 30.0,
                'idle_normalized_threshold': 0.3,
                'ctx_rate_per_rank_threshold': 200,
                'sync_variance_threshold': 15.0,
                'compute_power_baseline': 20.0,
                'comm_power_baseline': 10.0,
                'cpu_utilization_per_rank_threshold': 0.5,
            }
        elif self.expected_ranks <= 16:
            self.dynamic_thresholds = {
                'power_per_rank_threshold': 15.0,
                'idle_normalized_threshold': 0.5,
                'ctx_rate_per_rank_threshold': 100,
                'sync_variance_threshold': 10.0,
                'compute_power_baseline': 12.0,
                'comm_power_baseline': 8.0,
                'cpu_utilization_per_rank_threshold': 0.3,
            }
        else:
            self.dynamic_thresholds = {
                'power_per_rank_threshold': 10.0,
                'idle_normalized_threshold': 0.7,
                'ctx_rate_per_rank_threshold': 50,
                'sync_variance_threshold': 5.0,
                'compute_power_baseline': 8.0,
                'comm_power_baseline': 6.0,
                'cpu_utilization_per_rank_threshold': 0.1,
            }
    
    def estimate_expected_behavior(self):
        """Estimate expected behavior based on empirical scaling data"""
        available_ranks = sorted(EMPIRICAL_SCALING_DATA.keys())
        nearest = min(available_ranks, key=lambda x: abs(x - self.expected_ranks))
        
        if nearest == self.expected_ranks:
            self.expected_comm_pct = EMPIRICAL_SCALING_DATA[nearest]["comm_pct"]
            self.expected_compute_pct = EMPIRICAL_SCALING_DATA[nearest]["compute_pct"]
        else:
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
    
    def detect_network_interfaces(self):
        """Detect available network interfaces for monitoring"""
        interfaces = []
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()[2:]
                for line in lines:
                    if ":" in line:
                        iface = line.split(":")[0].strip()
                        if (iface in NETWORK_INTERFACES or 
                            iface.startswith("ib") or 
                            iface.startswith("eth") or
                            iface.startswith("eno") or
                            iface.startswith("ens") or
                            iface.startswith("bond")):
                            interfaces.append(iface)
        except Exception as e:
            print(f"[WARN] Could not read /proc/net/dev: {e}")
        
        interfaces = [iface for iface in interfaces if iface != 'lo']
        return interfaces
    
    def get_total_cores(self):
        """Get total number of CPU cores in the system"""
        try:
            with open("/proc/cpuinfo", "r") as f:
                content = f.read()
                return content.count("processor\t:")
        except:
            return 32  # Default assumption
    
    def normalize_cpu_metrics(self, cpu_metrics, actual_ranks):
        """Normalize CPU metrics based on actual MPI rank count"""
        user_pct, system_pct, idle_pct, iowait_pct = cpu_metrics
        
        total_cores = self.get_total_cores()
        
        if actual_ranks > 0 and total_cores > 0:
            max_possible_util = (actual_ranks / total_cores) * 100
            expected_idle = 100 - max_possible_util
            normalized_idle = max(0, (idle_pct - expected_idle) / max_possible_util * 100) if max_possible_util > 0 else 0
            effective_cpu_util = (100 - idle_pct) / max_possible_util * 100 if max_possible_util > 0 else 0
            per_rank_cpu = (100 - idle_pct) / actual_ranks if actual_ranks > 0 else 0
        else:
            normalized_idle = idle_pct
            effective_cpu_util = 100 - idle_pct
            per_rank_cpu = 0
        
        return {
            'user_pct': user_pct,
            'system_pct': system_pct,
            'idle_pct': idle_pct,
            'iowait_pct': iowait_pct,
            'normalized_idle': normalized_idle,
            'effective_cpu_util': effective_cpu_util,
            'per_rank_cpu': per_rank_cpu,
            'active_cpu': 100 - idle_pct
        }
    
    def normalize_power_metrics(self, pkg_power_W, dram_power_W, actual_ranks):
        """Normalize power metrics based on actual MPI rank count"""
        if actual_ranks > 0:
            power_per_rank = pkg_power_W / actual_ranks
            dram_power_per_rank = dram_power_W / actual_ranks if dram_power_W > 0 else 0
        else:
            power_per_rank = pkg_power_W
            dram_power_per_rank = dram_power_W
        
        if pkg_power_W > 0:
            power_efficiency = (actual_ranks * 100) / pkg_power_W if actual_ranks > 0 else 0
        else:
            power_efficiency = 0
        
        return {
            'total_power': pkg_power_W,
            'dram_power': dram_power_W,
            'power_per_rank': power_per_rank,
            'dram_power_per_rank': dram_power_per_rank,
            'power_efficiency': power_efficiency
        }
    
    def get_miniMD_pids(self):
        """Get PIDs of running miniMD processes and detect actual rank count"""
        try:
            result = subprocess.run(["pgrep", "-f", "miniMD"],
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                pids = [int(pid) for pid in result.stdout.strip().split()]
                
                if len(pids) > 0 and self.actual_detected_ranks is None:
                    self.actual_detected_ranks = len(pids)
                    print(f"[INFO] Detected {self.actual_detected_ranks} running miniMD processes")
                    
                    if self.actual_detected_ranks in EMPIRICAL_SCALING_DATA:
                        self.expected_comm_pct = EMPIRICAL_SCALING_DATA[self.actual_detected_ranks]["comm_pct"]
                        self.expected_compute_pct = EMPIRICAL_SCALING_DATA[self.actual_detected_ranks]["compute_pct"]
                        print(f"[INFO] Updated expectations: {self.actual_detected_ranks} ranks => "
                              f"{self.expected_comm_pct:.1f}% comm, {self.expected_compute_pct:.1f}% compute")
                
                return pids
            else:
                try:
                    result = subprocess.run(["ps", "-ef"], capture_output=True, text=True, timeout=5)
                    pids = []
                    for line in result.stdout.split('\n'):
                        if 'miniMD' in line and 'grep' not in line:
                            parts = line.split()
                            if len(parts) >= 2:
                                try:
                                    pids.append(int(parts[1]))
                                except ValueError:
                                    pass
                    
                    if len(pids) > 0 and self.actual_detected_ranks is None:
                        self.actual_detected_ranks = len(pids)
                        print(f"[INFO] Detected {self.actual_detected_ranks} running miniMD processes")
                    
                    return pids
                except Exception:
                    pass
        except subprocess.TimeoutExpired:
            print("[WARN] pgrep timed out, trying alternative PID detection")
        except Exception as e:
            print(f"[WARN] PID detection failed: {e}")
        
        return []
    
    def safe_delta(self, current, previous, counter_name=""):
        """Safe delta calculation with counter wrap-around handling"""
        if current >= previous:
            return current - previous
        
        if "energy_uj" in counter_name.lower() or "rapl" in counter_name.lower():
            max_value = 2**32
        elif "ctx" in counter_name.lower():
            max_value = 2**64
        elif "net" in counter_name.lower():
            max_value = 2**64
        else:
            max_value = 2**32
        
        delta = (max_value - previous) + current
        return delta
    
    def safe_rate(self, curr, prev, dt, counter_name=""):
        """Calculate rate with sanity clamping"""
        if dt <= 0:
            return 0.0
        
        delta = self.safe_delta(curr, prev, counter_name)
        rate = delta / dt
        
        if rate > MAX_CTX_RATE:
            return 0.0
        return rate
    
    def read_rapl_energy(self, path, counter_name=""):
        """Read RAPL energy counter (microjoules) with error handling"""
        if not path or not os.path.exists(path):
            return 0
        
        try:
            with open(path, "r") as f:
                return int(f.read().strip())
        except PermissionError as e:
            print(f"[ERROR] Permission denied reading RAPL at {path}: {e}")
            return 0
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
                        parts = line.replace(':', ' ').split()
                        if len(parts) >= 17:
                            iface = parts[0]
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
                    if len(stat_data) >= 15:
                        utime = int(stat_data[13])
                        stime = int(stat_data[14])
                        cpu_times[pid] = utime + stime
            except (FileNotFoundError, ValueError, IndexError, PermissionError):
                continue
        return cpu_times
    
    def calculate_sync_variance(self, curr_cpu_times, prev_cpu_times):
        """Calculate synchronization variance across processes"""
        deltas = []
        for pid, curr_time in curr_cpu_times.items():
            if pid in prev_cpu_times:
                prev_time = prev_cpu_times[pid]
                delta = max(curr_time - prev_time, 0)
                if delta > 0:
                    deltas.append(delta)
        
        if len(deltas) < 2:
            return 0.0
        
        try:
            mean = statistics.mean(deltas)
            if mean == 0:
                return 0.0
            
            if len(deltas) > 1:
                sum_sq_diff = sum((x - mean) ** 2 for x in deltas)
                variance = sum_sq_diff / (len(deltas) - 1)
            else:
                variance = 0
            
            if mean > 0:
                normalized_variance = (variance / mean * 100) if variance > 0 else 0
            else:
                normalized_variance = 0
            
            return normalized_variance
        except (statistics.StatisticsError, ZeroDivisionError):
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
            except (FileNotFoundError, ValueError, IndexError, PermissionError):
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
    
    def update_dynamic_thresholds(self, normalized_metrics, actual_ranks, timestamp):
        """Update thresholds based on observed patterns and rank count"""
        self.power_pattern.append(normalized_metrics.get('power_per_rank', 0))
        self.cpu_pattern.append(normalized_metrics.get('per_rank_cpu', 0))
        
        if actual_ranks <= 4:
            self.dynamic_thresholds['power_per_rank_threshold'] = 25.0
            self.dynamic_thresholds['cpu_utilization_per_rank_threshold'] = 0.4
        elif actual_ranks <= 16:
            self.dynamic_thresholds['power_per_rank_threshold'] = 15.0
            self.dynamic_thresholds['cpu_utilization_per_rank_threshold'] = 0.25
        else:
            self.dynamic_thresholds['power_per_rank_threshold'] = 10.0
            self.dynamic_thresholds['cpu_utilization_per_rank_threshold'] = 0.15
    
    def classify_with_rank_aware_algorithms(self, pkg_power_W, dram_power_W, cpu_metrics,
                                           ctx_switch_rate, pids_count, network_bw,
                                           sync_variance_norm, timestamp):
        """
        Rank-aware classification using normalized metrics
        """
        actual_ranks = self.actual_detected_ranks or pids_count or self.expected_ranks
        
        normalized_cpu = self.normalize_cpu_metrics(cpu_metrics, actual_ranks)
        normalized_power = self.normalize_power_metrics(pkg_power_W, dram_power_W, actual_ranks)
        
        net_recv_mbps, net_sent_mbps = network_bw
        total_net_bw = net_recv_mbps + net_sent_mbps
        
        ctx_per_rank = ctx_switch_rate / actual_ranks if actual_ranks > 0 else 0
        
        self.update_dynamic_thresholds({
            **normalized_cpu,
            **normalized_power,
            'ctx_per_rank': ctx_per_rank,
            'sync_variance': sync_variance_norm
        }, actual_ranks, timestamp)
        
        comm_score = 0.0
        comp_score = 0.0
        reasons = []
        
        power_per_rank = normalized_power['power_per_rank']
        if power_per_rank > self.dynamic_thresholds['power_per_rank_threshold'] * 1.2:
            comp_score += 2.0
            reasons.append(f"high_pwr_per_rank={power_per_rank:.1f}W")
        elif power_per_rank < self.dynamic_thresholds['power_per_rank_threshold'] * 0.8:
            comm_score += 1.5
            reasons.append(f"low_pwr_per_rank={power_per_rank:.1f}W")
        
        per_rank_cpu = normalized_cpu['per_rank_cpu']
        if per_rank_cpu > self.dynamic_thresholds['cpu_utilization_per_rank_threshold'] * 100:
            comp_score += 2.0
            reasons.append(f"high_cpu_per_rank={per_rank_cpu:.1f}%")
        elif per_rank_cpu < self.dynamic_thresholds['cpu_utilization_per_rank_threshold'] * 100 * 0.5:
            comm_score += 1.5
            reasons.append(f"low_cpu_per_rank={per_rank_cpu:.1f}%")
        
        effective_cpu = normalized_cpu['effective_cpu_util']
        if effective_cpu > 50:
            comp_score += 1.5
            reasons.append(f"effective_cpu={effective_cpu:.1f}%")
        elif effective_cpu < 20:
            comm_score += 1.0
            reasons.append(f"low_effective_cpu={effective_cpu:.1f}%")
        
        if actual_ranks >= 16 and total_net_bw > 5:
            comm_score += 1.0
            reasons.append(f"net_bw={total_net_bw:.1f}MB/s")
        
        if ctx_per_rank > self.dynamic_thresholds.get('ctx_rate_per_rank_threshold', 100):
            comm_score += 1.0
            reasons.append(f"high_ctx_per_rank={ctx_per_rank:.0f}/s")
        
        if sync_variance_norm < self.dynamic_thresholds.get('sync_variance_threshold', 10):
            comm_score += 0.5
            reasons.append(f"low_sync={sync_variance_norm:.1f}%")
        elif sync_variance_norm > self.dynamic_thresholds.get('sync_variance_threshold', 10) * 2:
            comp_score += 0.5
            reasons.append(f"high_sync={sync_variance_norm:.1f}%")
        
        # Apply empirical expectations
        if actual_ranks in EMPIRICAL_SCALING_DATA:
            expected_comm = EMPIRICAL_SCALING_DATA[actual_ranks]["comm_pct"] / 100
            expected_comp = EMPIRICAL_SCALING_DATA[actual_ranks]["compute_pct"] / 100
        else:
            expected_comm = self.expected_comm_pct / 100
            expected_comp = self.expected_compute_pct / 100
        
        comm_score_weighted = comm_score * (1.0 + expected_comm)
        comp_score_weighted = comp_score * (1.0 + expected_comp)
        
        phase_diff = comm_score_weighted - comp_score_weighted
        
        phase = "MIXED"
        confidence = 0.0
        
        if actual_ranks <= 4:
            if phase_diff > 3.0:
                phase = "COMMUNICATION"
                confidence = min(0.3 + phase_diff / 15.0, 1.0)
            elif phase_diff < -1.5:
                phase = "COMPUTE"
                confidence = min(0.3 + abs(phase_diff) / 10.0, 1.0)
        elif actual_ranks <= 16:
            if phase_diff > 2.0:
                phase = "COMMUNICATION"
                confidence = min(0.3 + phase_diff / 10.0, 1.0)
            elif phase_diff < -2.0:
                phase = "COMPUTE"
                confidence = min(0.3 + abs(phase_diff) / 10.0, 1.0)
        else:
            if phase_diff > 1.5:
                phase = "COMMUNICATION"
                confidence = min(0.4 + phase_diff / 8.0, 1.0)
            elif phase_diff < -3.0:
                phase = "COMPUTE"
                confidence = min(0.4 + abs(phase_diff) / 12.0, 1.0)
        
        if normalized_cpu['iowait_pct'] > 15:
            phase = "IO_WAIT"
            confidence = 0.7
            reasons.append(f"iowait={normalized_cpu['iowait_pct']:.1f}%")
        elif total_net_bw > 20:
            phase = "NETWORK_COMM"
            confidence = 0.6
            reasons.append(f"high_net={total_net_bw:.1f}MB/s")
        elif normalized_cpu['idle_pct'] > 98:
            phase = "IDLE"
            confidence = 0.8
            reasons.append(f"very_idle={normalized_cpu['idle_pct']:.0f}%")
        
        phase = self.stabilize_phase(phase, timestamp)
        
        details = {
            'phase': phase,
            'confidence': confidence,
            'total_power_W': normalized_power['total_power'],
            'power_per_rank_W': normalized_power['power_per_rank'],
            'dram_power_W': normalized_power['dram_power'],
            'cpu_user_pct': normalized_cpu['user_pct'],
            'cpu_system_pct': normalized_cpu['system_pct'],
            'cpu_idle_pct': normalized_cpu['idle_pct'],
            'cpu_iowait_pct': normalized_cpu['iowait_pct'],
            'per_rank_cpu_pct': normalized_cpu['per_rank_cpu'],
            'effective_cpu_util_pct': normalized_cpu['effective_cpu_util'],
            'normalized_idle_pct': normalized_cpu['normalized_idle'],
            'voluntary_ctx_switches_per_sec': ctx_switch_rate,
            'ctx_per_rank_per_sec': ctx_per_rank,
            'active_mpi_ranks': actual_ranks,
            'net_recv_mbps': net_recv_mbps,
            'net_sent_mbps': net_sent_mbps,
            'sync_variance_pct': sync_variance_norm,
            'comm_score': comm_score,
            'comp_score': comp_score,
            'comm_score_weighted': comm_score_weighted,
            'comp_score_weighted': comp_score_weighted,
            'phase_diff': phase_diff,
            'power_efficiency': normalized_power['power_efficiency'],
            'reasons': ','.join(reasons) if reasons else 'balanced'
        }
        
        return phase, details
    
    def stabilize_phase(self, proposed_phase, timestamp):
        """Apply phase stabilization using history"""
        self.phase_sequence.append(proposed_phase)
        if len(self.phase_sequence) > 10:
            self.phase_sequence.pop(0)
        
        if len(self.phase_sequence) >= 3:
            recent = self.phase_sequence[-3:]
            counts = Counter(recent)
            most_common = counts.most_common(1)[0]
            
            if most_common[1] >= 2 and most_common[0] != proposed_phase:
                return most_common[0]
        
        return proposed_phase
    
    def track_phase_duration(self, phase, timestamp):
        """Track phase duration with enhanced statistics"""
        current_time = timestamp
        
        if self.current_phase is None:
            self.current_phase = phase
            self.phase_start_time = current_time
        elif self.current_phase != phase:
            if self.phase_start_time is not None:
                duration = current_time - self.phase_start_time
                if duration > 0:
                    self.phase_stats[self.current_phase]["time"] += duration
                    self.phase_stats[self.current_phase]["samples"] += 1
                    self.phase_stats[self.current_phase]["transitions"] += 1
                    
                    self.phase_durations_history[self.current_phase].append(duration)
                    
                    if self.current_phase == "COMPUTE":
                        self.compute_bursts.append(duration)
                    elif self.current_phase == "COMMUNICATION":
                        self.communication_intervals.append(duration)
                    
                    self.phase_change_count += 1
                    self.last_phase_change_time = current_time
            
            self.current_phase = phase
            self.phase_start_time = current_time
    
    def setup_csv(self):
        """Initialize CSV with rank-aware metrics"""
        try:
            self.csv_file = open(LOG_FILE, "w", newline='')
            fieldnames = [
                'timestamp',
                'phase',
                'confidence',
                'total_power_W',
                'power_per_rank_W',
                'dram_power_W',
                'cpu_user_pct',
                'cpu_system_pct', 
                'cpu_idle_pct',
                'cpu_iowait_pct',
                'per_rank_cpu_pct',
                'effective_cpu_util_pct',
                'normalized_idle_pct',
                'voluntary_ctx_switches_per_sec',
                'ctx_per_rank_per_sec',
                'active_mpi_ranks',
                'net_recv_mbps',
                'net_sent_mbps',
                'sync_variance_pct',
                'comm_score',
                'comp_score',
                'comm_score_weighted',
                'comp_score_weighted',
                'phase_diff',
                'power_efficiency',
                'pkg_energy_J',
                'dram_energy_J',
                'reasons'
            ]
            self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
            self.writer.writeheader()
            print(f"[INFO] Logging to {LOG_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to create CSV file: {e}")
            raise
    
    def run_monitoring(self, miniMD_process=None):
        """Main monitoring loop with rank-aware classification"""
        print(f"[INFO] Starting INTELLIGENT communication phase monitoring v15.0")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        
        self.setup_csv()
        
        self.prev_energy = self.read_rapl_energy(RAPL_PATH, "pkg_energy")
        if self.has_dram_rapl:
            self.prev_dram_energy = self.read_rapl_energy(RAPL_DRAM_PATH, "dram_energy")
        
        self.prev_proc_stats = self.read_proc_stat()
        self.prev_net_stats = self.read_network_stats()
        self.prev_ctx_switches = (0, 0)
        self.prev_per_process_cpu = {}
        
        self.start_time = time.time()
        sample_count = 0
        
        try:
            while True:
                if miniMD_process and miniMD_process.poll() is not None:
                    print("[INFO] miniMD process completed")
                    break
                
                pids = self.get_miniMD_pids()
                if not pids and miniMD_process is None:
                    print("[INFO] No miniMD processes detected")
                    break
                
                sample_count += 1
                current_time = time.time() - self.start_time
                
                time.sleep(SAMPLE_INTERVAL)
                
                energy_now = self.read_rapl_energy(RAPL_PATH, "pkg_energy")
                delta_pkg_energy = self.safe_delta(energy_now, self.prev_energy, "pkg_energy_uj") / 1e6
                self.prev_energy = energy_now
                
                pkg_power_W = delta_pkg_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                dram_power_W = 0
                delta_dram_energy = 0
                if self.has_dram_rapl:
                    dram_energy_now = self.read_rapl_energy(RAPL_DRAM_PATH, "dram_energy")
                    delta_dram_energy = self.safe_delta(dram_energy_now, self.prev_dram_energy, "dram_energy_uj") / 1e6
                    self.prev_dram_energy = dram_energy_now
                    dram_power_W = delta_dram_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                curr_proc_stats = self.read_proc_stat()
                cpu_metrics = self.calculate_cpu_usage(self.prev_proc_stats, curr_proc_stats)
                self.prev_proc_stats = curr_proc_stats
                
                curr_net_stats = self.read_network_stats()
                net_recv_mbps, net_sent_mbps = self.calculate_network_bandwidth(self.prev_net_stats, curr_net_stats)
                self.prev_net_stats = curr_net_stats
                
                curr_ctx_switches = self.get_proc_context_switches(pids)
                voluntary_ctx_rate = self.safe_rate(
                    curr_ctx_switches[0], self.prev_ctx_switches[0],
                    SAMPLE_INTERVAL, "voluntary_ctx"
                )
                self.prev_ctx_switches = curr_ctx_switches
                
                curr_per_process_cpu = self.get_per_process_cpu_times(pids)
                sync_variance_norm = self.calculate_sync_variance(curr_per_process_cpu, self.prev_per_process_cpu)
                self.prev_per_process_cpu = curr_per_process_cpu
                
                phase, details = self.classify_with_rank_aware_algorithms(
                    pkg_power_W, dram_power_W, cpu_metrics,
                    voluntary_ctx_rate, len(pids),
                    (net_recv_mbps, net_sent_mbps),
                    sync_variance_norm, current_time
                )
                
                self.track_phase_duration(phase, current_time)
                
                row = {
                    'timestamp': f"{current_time:.2f}",
                    'phase': details['phase'],
                    'confidence': f"{details['confidence']:.3f}",
                    'total_power_W': f"{details['total_power_W']:.1f}",
                    'power_per_rank_W': f"{details['power_per_rank_W']:.1f}",
                    'dram_power_W': f"{details['dram_power_W']:.1f}",
                    'cpu_user_pct': f"{details['cpu_user_pct']:.1f}",
                    'cpu_system_pct': f"{details['cpu_system_pct']:.1f}",
                    'cpu_idle_pct': f"{details['cpu_idle_pct']:.1f}",
                    'cpu_iowait_pct': f"{details['cpu_iowait_pct']:.1f}",
                    'per_rank_cpu_pct': f"{details['per_rank_cpu_pct']:.1f}",
                    'effective_cpu_util_pct': f"{details['effective_cpu_util_pct']:.1f}",
                    'normalized_idle_pct': f"{details['normalized_idle_pct']:.1f}",
                    'voluntary_ctx_switches_per_sec': f"{details['voluntary_ctx_switches_per_sec']:.0f}",
                    'ctx_per_rank_per_sec': f"{details['ctx_per_rank_per_sec']:.0f}",
                    'active_mpi_ranks': details['active_mpi_ranks'],
                    'net_recv_mbps': f"{details['net_recv_mbps']:.1f}",
                    'net_sent_mbps': f"{details['net_sent_mbps']:.1f}",
                    'sync_variance_pct': f"{details['sync_variance_pct']:.1f}",
                    'comm_score': f"{details['comm_score']:.2f}",
                    'comp_score': f"{details['comp_score']:.2f}",
                    'comm_score_weighted': f"{details['comm_score_weighted']:.2f}",
                    'comp_score_weighted': f"{details['comp_score_weighted']:.2f}",
                    'phase_diff': f"{details['phase_diff']:.2f}",
                    'power_efficiency': f"{details['power_efficiency']:.3f}",
                    'pkg_energy_J': f"{delta_pkg_energy:.4f}",
                    'dram_energy_J': f"{delta_dram_energy:.4f}",
                    'reasons': details['reasons']
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                if sample_count % 10 == 0:
                    phase_char = details['phase'][0] if details['phase'] else '?'
                    actual_ranks = details['active_mpi_ranks']
                    print(f"[{current_time:6.1f}s] {phase_char} "
                          f"R:{actual_ranks:2d} "
                          f"Pwr/R:{details['power_per_rank_W']:4.1f}W "
                          f"CPU/R:{details['per_rank_cpu_pct']:3.0f}% "
                          f"Eff:{details['effective_cpu_util_pct']:3.0f}% "
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
    
    def generate_comprehensive_report(self, total_time, miniMD_output=None):
        """Generate comprehensive analysis report with optimization insights"""
        try:
            with open(SUMMARY_FILE, "w") as f:
                f.write("="*80 + "\n")
                f.write("INTELLIGENT MiniMD Communication Phase Analysis - Version 15.0\n")
                f.write("="*80 + "\n\n")
                
                # Optimization profile info
                f.write("OPTIMIZATION PROFILE:\n")
                f.write("-"*50 + "\n")
                profile_info = OPTIMIZATION_PROFILES[self.optimization_manager.profile]
                f.write(f"Profile: {self.optimization_manager.profile}\n")
                f.write(f"Description: {profile_info['description']}\n")
                f.write(f"Recommended for: {profile_info['recommended_for']}\n")
                f.write(f"Command: {' '.join(self.cmd)}\n")
                if self.env_vars:
                    f.write(f"Environment variables: {self.env_vars}\n")
                f.write("\n")
                
                # Empirical scaling context
                f.write("EMPIRICAL SCALING CONTEXT:\n")
                f.write("-"*50 + "\n")
                actual_ranks = self.actual_detected_ranks or self.expected_ranks
                f.write(f"Expected MPI ranks: {self.expected_ranks}\n")
                f.write(f"Detected MPI ranks: {actual_ranks}\n")
                f.write(f"Expected communication: {self.expected_comm_pct:.1f}%\n")
                f.write(f"Expected compute: {self.expected_compute_pct:.1f}%\n")
                f.write("\n")
                
                # Phase analysis
                f.write("PHASE ANALYSIS:\n")
                f.write("-"*50 + "\n")
                
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
                    
                    f.write(f"{phase:25s}: {duration:8.2f}s ({percentage:6.2f}%) | "
                           f"Samples: {samples:5d} | Avg: {avg_duration:6.3f}s | "
                           f"Transitions: {stats['transitions']}\n")
                
                f.write(f"\nTotal monitored time: {total_time:.2f}s\n")
                f.write(f"Total tracked phase time: {total_tracked:.2f}s\n")
                f.write(f"Total phase changes: {self.phase_change_count}\n")
                
                # Performance validation
                if "COMMUNICATION" in self.phase_stats:
                    comm_time = self.phase_stats["COMMUNICATION"]["time"]
                    comm_pct = (comm_time / total_time * 100) if total_time > 0 else 0
                    
                    f.write("\nPERFORMANCE VALIDATION:\n")
                    f.write("-"*50 + "\n")
                    f.write(f"Detected communication: {comm_pct:.2f}%\n")
                    f.write(f"Expected communication: {self.expected_comm_pct:.1f}%\n")
                    
                    diff = comm_pct - self.expected_comm_pct
                    f.write(f"Difference: {diff:+.2f}%\n")
                    
                    if abs(diff) < 5:
                        f.write("✓ Excellent match with expectations!\n")
                    elif abs(diff) < 10:
                        f.write("✓ Good match with expectations\n")
                    elif abs(diff) < 20:
                        f.write("~ Reasonable match\n")
                    else:
                        f.write("⚠ Significant deviation from expectations\n")
                
                # Optimization recommendations
                f.write("\n" + "="*80 + "\n")
                f.write("OPTIMIZATION RECOMMENDATIONS:\n")
                f.write("="*80 + "\n\n")
                
                actual_ranks = self.actual_detected_ranks or self.expected_ranks
                recommendations = self.optimization_manager.recommend_profile(actual_ranks)
                
                f.write("Based on your configuration ({} ranks), consider these profiles:\n".format(actual_ranks))
                for i, (profile, reason) in enumerate(recommendations, 1):
                    profile_info = OPTIMIZATION_PROFILES[profile]
                    f.write(f"\n{i}. {profile}:\n")
                    f.write(f"   Description: {profile_info['description']}\n")
                    f.write(f"   Reason: {reason}\n")
                    f.write(f"   Command: mpirun -np {actual_ranks} {' '.join(profile_info['mpi_args'])} ./miniMD_openmpi -i in.lj.miniMD\n")
                
                # Compute burst analysis
                if "COMPUTE" in self.phase_stats and self.compute_bursts:
                    f.write("\nCOMPUTE BURST ANALYSIS:\n")
                    f.write("-"*50 + "\n")
                    f.write(f"Number of compute bursts: {len(self.compute_bursts)}\n")
                    if self.compute_bursts:
                        avg_burst = sum(self.compute_bursts) / len(self.compute_bursts)
                        f.write(f"Average burst duration: {avg_burst:.3f}s\n")
                        f.write(f"Min/Max burst: {min(self.compute_bursts):.3f}s / {max(self.compute_bursts):.3f}s\n")
                
                # Rank-specific analysis
                f.write("\n" + "="*80 + "\n")
                f.write("RANK-AWARE ANALYSIS:\n")
                f.write("="*80 + "\n\n")
                
                if actual_ranks <= 4:
                    f.write("WORKLOAD CHARACTERIZATION: COMPUTE-BOUND (Low Ranks)\n")
                    f.write("  - Problem size per rank is large\n")
                    f.write("  - Communication overhead is relatively low (20-40%)\n")
                    f.write("  - Focus on computational efficiency\n")
                    f.write("\nRECOMMENDED OPTIMIZATIONS:\n")
                    f.write("  - Use hybrid MPI+OpenMP to reduce MPI communication\n")
                    f.write("  - Enable core affinity for better cache locality\n")
                    f.write("  - Consider increasing problem size per rank\n")
                elif actual_ranks <= 16:
                    f.write("WORKLOAD CHARACTERIZATION: BALANCED (Medium Ranks)\n")
                    f.write("  - Good balance between computation and communication\n")
                    f.write("  - Communication percentage: 40-75%\n")
                    f.write("  - Both aspects important for performance\n")
                    f.write("\nRECOMMENDED OPTIMIZATIONS:\n")
                    f.write("  - Tune MPI parameters for Ethernet communication\n")
                    f.write("  - Use moderate OpenMP threading (2-4 threads per rank)\n")
                    f.write("  - Optimize collective operations\n")
                else:
                    f.write("WORKLOAD CHARACTERIZATION: COMMUNICATION-BOUND (High Ranks)\n")
                    f.write("  - Communication overhead dominates (75-97%)\n")
                    f.write("  - Problem size per rank is small\n")
                    f.write("  - Limited by MPI communication latency/bandwidth\n")
                    f.write("\nRECOMMENDED OPTIMIZATIONS:\n")
                    f.write("  - Network optimization (buffer sizes, protocols)\n")
                    f.write("  - Reduce MPI rank count, increase OpenMP threads\n")
                    f.write("  - Optimize message sizes and collective algorithms\n")
                
                # Next steps
                f.write("\n" + "="*80 + "\n")
                f.write("NEXT STEPS FOR PERFORMANCE ANALYSIS:\n")
                f.write("="*80 + "\n\n")
                f.write("1. Test different optimization profiles:\n")
                f.write("   python script.py --profile tuned --ranks 16\n")
                f.write("   python script.py --profile hybrid_4threads --ranks 8\n")
                f.write("2. Compare results across profiles\n")
                f.write(f"3. Check detailed scaling analysis: {SCALING_FILE}\n")
                f.write(f"4. Review optimization report: {OPTIMIZATION_FILE}\n")
                f.write("5. Try different rank counts to find optimal configuration\n")
            
            print(f"[INFO] Comprehensive report saved to {SUMMARY_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to generate report: {e}")
    
    def generate_optimization_report(self):
        """Generate detailed optimization report"""
        actual_ranks = self.actual_detected_ranks or self.expected_ranks
        comm_time = self.phase_stats.get("COMMUNICATION", {"time": 0})["time"]
        total_time = time.time() - self.start_time if self.start_time else 1
        comm_pct = (comm_time / total_time * 100) if total_time > 0 else 0
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "optimization_profile": self.optimization_manager.profile,
            "profile_description": OPTIMIZATION_PROFILES[self.optimization_manager.profile]["description"],
            "ranks": actual_ranks,
            "command": " ".join(self.cmd),
            "environment_variables": self.env_vars,
            "performance": {
                "total_time": total_time,
                "communication_time": comm_time,
                "communication_percentage": comm_pct,
                "expected_communication": self.expected_comm_pct,
                "difference": comm_pct - self.expected_comm_pct,
                "phase_distribution": dict(self.phase_stats),
                "phase_transitions": self.phase_change_count
            },
            "recommendations": []
        }
        
        # Generate recommendations
        if actual_ranks <= 4 and comm_pct > 40:
            report["recommendations"].append({
                "type": "hybrid",
                "message": "High communication percentage for low ranks. Consider hybrid MPI+OpenMP.",
                "suggested_profile": "hybrid_8threads"
            })
        elif actual_ranks >= 16 and comm_pct > 90:
            report["recommendations"].append({
                "type": "network",
                "message": "Extremely communication-bound. Network optimization crucial.",
                "suggested_profile": "network_optimized"
            })
        
        # Save report
        try:
            with open(OPTIMIZATION_FILE, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"[INFO] Optimization report saved to {OPTIMIZATION_FILE}")
        except Exception as e:
            print(f"[WARN] Failed to save optimization report: {e}")
        
        return report
    
    def generate_scaling_analysis(self):
        """Generate scaling analysis based on empirical data and current run"""
        scaling_data = {
            "empirical_data": EMPIRICAL_SCALING_DATA,
            "current_run": {
                "expected_ranks": self.expected_ranks,
                "detected_ranks": self.actual_detected_ranks,
                "expected_comm_pct": self.expected_comm_pct,
                "expected_compute_pct": self.expected_compute_pct,
                "optimization_profile": self.optimization_manager.profile
            },
            "analysis": {}
        }
        
        actual_ranks = self.actual_detected_ranks or self.expected_ranks
        
        if actual_ranks >= 2:
            base_ranks = 2
            if base_ranks in EMPIRICAL_SCALING_DATA and actual_ranks in EMPIRICAL_SCALING_DATA:
                base_time = 10.0
                base_comm = EMPIRICAL_SCALING_DATA[base_ranks]["comm_pct"]
                current_comm = EMPIRICAL_SCALING_DATA[actual_ranks]["comm_pct"]
                
                current_time = base_time * (current_comm / 100) / (base_comm / 100)
                
                ideal_speedup = actual_ranks / base_ranks
                actual_speedup = base_time / current_time
                scaling_efficiency = (actual_speedup / ideal_speedup) * 100
                
                scaling_data["analysis"]["scaling_efficiency"] = scaling_efficiency
                scaling_data["analysis"]["actual_speedup"] = actual_speedup
                scaling_data["analysis"]["ideal_speedup"] = ideal_speedup
                scaling_data["analysis"]["estimated_time"] = current_time
        
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
        
        if self.current_phase and self.phase_start_time:
            final_duration = total_time - self.phase_start_time
            if final_duration > 0:
                self.phase_stats[self.current_phase]["time"] += final_duration
                self.phase_stats[self.current_phase]["samples"] += 1
        
        if self.csv_file:
            try:
                self.csv_file.close()
            except Exception as e:
                print(f"[WARN] Error closing CSV file: {e}")
        
        # Generate reports
        scaling_data = self.generate_scaling_analysis()
        optimization_report = self.generate_optimization_report()
        self.generate_comprehensive_report(total_time)
        
        print(f"\n[INFO] Monitoring complete. Data saved to {LOG_FILE}")
        print(f"[INFO] Summary report saved to {SUMMARY_FILE}")
        print(f"[INFO] Scaling analysis saved to {SCALING_FILE}")
        print(f"[INFO] Optimization report saved to {OPTIMIZATION_FILE}")
        
        # Print quick summary
        print("\n" + "="*60)
        print("QUICK SUMMARY:")
        print("="*60)
        for phase in sorted(self.phase_stats.keys()):
            stats = self.phase_stats[phase]
            duration = stats["time"]
            percentage = (duration / total_time * 100) if total_time > 0 else 0
            print(f"{phase:25s}: {duration:7.2f}s ({percentage:6.2f}%)")

def list_profiles():
    """List all available optimization profiles"""
    print("="*80)
    print("AVAILABLE OPTIMIZATION PROFILES:")
    print("="*80)
    for profile_name, profile_info in OPTIMIZATION_PROFILES.items():
        print(f"\n{profile_name}:")
        print(f"  Description: {profile_info['description']}")
        print(f"  Recommended for: {profile_info['recommended_for']}")
        if profile_info['env_vars']:
            print(f"  Environment variables: {profile_info['env_vars']}")

def main():
    """Entry point with enhanced argument parsing"""
    parser = argparse.ArgumentParser(
        description='INTELLIGENT MiniMD Communication Phase Monitor with Optimizations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic run with 32 ranks, default profile
  python script.py --ranks 32
  
  # Tuned profile for Ethernet optimization
  python script.py --profile tuned --ranks 16
  
  # Hybrid MPI+OpenMP with 4 threads per rank
  python script.py --profile hybrid_4threads --ranks 8
  
  # Network optimized for high rank counts
  python script.py --profile network_optimized --ranks 32
  
  # List all available profiles
  python script.py --list-profiles
        """
    )
    
    parser.add_argument('-n', '--ranks', type=int, default=32,
                       help='Number of MPI ranks (default: 32)')
    parser.add_argument('-p', '--profile', type=str, default='default',
                       choices=list(OPTIMIZATION_PROFILES.keys()),
                       help='Optimization profile (default: default)')
    parser.add_argument('-e', '--executable', type=str, default='./miniMD_openmpi',
                       help='miniMD executable path (default: ./miniMD_openmpi)')
    parser.add_argument('-i', '--input', type=str, default='in.lj.miniMD',
                       help='Input file (default: in.lj.miniMD)')
    parser.add_argument('-s', '--sample-interval', type=float, default=0.2,
                       help='Sampling interval in seconds (default: 0.2)')
    parser.add_argument('--list-profiles', action='store_true',
                       help='List all available optimization profiles and exit')
    parser.add_argument('--no-run', action='store_true',
                       help='Monitor existing miniMD process instead of launching one')
    parser.add_argument('-c', '--command', type=str,
                       help='Full command to run miniMD (overrides profile)')
    
    args = parser.parse_args()
    
    if args.list_profiles:
        list_profiles()
        return 0
    
    if not validate_environment():
        return 1
    
    print("="*80)
    print("INTELLIGENT MiniMD Communication Phase Monitor - Version 15.0")
    print("="*80)
    print("Key features:")
    print("  ✓ Multiple optimization profiles for different scenarios")
    print("  ✓ Process affinity and binding strategies")
    print("  ✓ MPI parameter tuning based on rank count")
    print("  ✓ Environment variable optimization")
    print("  ✓ Hybrid MPI+OpenMP support")
    print("="*80)
    print(f"Configuration:")
    print(f"  MPI ranks: {args.ranks}")
    print(f"  Optimization profile: {args.profile}")
    print(f"  Sample interval: {args.sample_interval}s")
    if args.ranks in EMPIRICAL_SCALING_DATA:
        print(f"  Empirical data: {EMPIRICAL_SCALING_DATA[args.ranks]['comm_pct']:.1f}% communication expected")
    print("="*80)
    
    global SAMPLE_INTERVAL
    SAMPLE_INTERVAL = args.sample_interval
    
    monitor = IntelligentCommPhaseMonitor(
        expected_ranks=args.ranks,
        optimization_profile=args.profile,
        executable=args.executable,
        input_file=args.input
    )
    
    try:
        if args.no_run:
            print("[INFO] Monitoring existing miniMD process (--no-run specified)")
            monitor.run_monitoring()
        else:
            if args.command:
                print(f"[INFO] Using custom command: {args.command}")
                import shlex
                cmd_parts = shlex.split(args.command)
                
                # Extract rank count from command
                for i, arg in enumerate(cmd_parts):
                    if arg in ['-np', '-n'] and i + 1 < len(cmd_parts):
                        try:
                            args.ranks = int(cmd_parts[i + 1])
                        except ValueError:
                            pass
                
                monitor.cmd = cmd_parts
                monitor.env_vars = {}
            
            # Set environment variables
            for key, value in monitor.env_vars.items():
                os.environ[key] = value
            
            print(f"[INFO] Launching miniMD with optimization: {' '.join(monitor.cmd)}")
            try:
                miniMD_process = subprocess.Popen(
                    monitor.cmd,
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
                stdout, stderr = miniMD_process.communicate(timeout=5)
                if stdout:
                    print("\n[INFO] MiniMD output:")
                    print(stdout[:2000])  # Print first 2000 chars
                if stderr:
                    print("\n[INFO] MiniMD errors:")
                    print(stderr[:1000])  # Print first 1000 chars
                    
            except FileNotFoundError as e:
                print(f"[ERROR] Failed to start miniMD: {e}")
                print(f"[ERROR] Command: {' '.join(monitor.cmd)}")
                print("[ERROR] Check if miniMD executable exists and is in PATH")
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
