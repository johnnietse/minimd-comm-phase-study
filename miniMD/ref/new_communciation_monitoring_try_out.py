#!/usr/bin/env python3
"""
Enhanced Communication Phase Monitor for miniMD
Focuses on actual MPI communication characteristics:
- MPI time vs compute time
- Message passing overhead
- Synchronization barriers
- Memory bandwidth during buffer operations
- Process coordination efficiency
"""

import subprocess
import time
import csv
import re
import os
from pathlib import Path
from collections import defaultdict

# ============ CONFIGURATION ============
CMD = ["mpirun", "--oversubscribe", "-np", "32", 
       "./miniMD_openmpi", "-i", "in.lj.miniMD"]
LOG_FILE = "comm_phase_detailed.csv"
SAMPLE_INTERVAL = 0.5  # Faster sampling for MPI events
PERF_PATH = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"

# Communication detection thresholds (based on MPI behavior)
IPC_COMM_THRESHOLD = 0.7        # Low IPC during MPI waits
CACHE_MISS_THRESHOLD = 0.35     # High misses during buffer ops
MPI_TIME_THRESHOLD = 0.15       # >15% time in MPI = comm phase
STALL_CYCLE_THRESHOLD = 0.4     # High stalls during synchronization

class CommPhaseMonitor:
    def __init__(self):
        self.proc = None
        self.csv_file = None
        self.writer = None
        self.start_time = None
        self.prev_energy = None
        self.mpi_time_estimate = 0.0
        
    def get_miniMD_pids(self):
        """Get PIDs of running miniMD processes"""
        try:
            result = subprocess.run(["pgrep", "-f", "miniMD"],
                                  capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                return [int(pid) for pid in result.stdout.strip().split()]
        except Exception as e:
            print(f"[ERROR] Failed to get PIDs: {e}")
        return []
    
    def read_energy(self):
        """Read CPU package energy (microjoules)"""
        try:
            with open(RAPL_PATH, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            print(f"[WARN] Could not read RAPL: {e}")
            return 0
    
    def parse_perf_output(self, output):
        """
        Parse perf stat output focusing on communication-relevant metrics:
        - Instructions & cycles (for IPC)
        - Cache misses (buffer operations)
        - Stalled cycles (synchronization overhead)
        - Branch misses (irregular MPI paths)
        - Memory bandwidth indicators
        """
        metrics = {
            'cycles': 0,
            'instructions': 0,
            'cache_misses': 0,
            'cache_refs': 0,
            'stalled_cycles': 0,
            'branch_misses': 0,
            'branches': 0,
            'context_switches': 0,  # Process coordination
            'cpu_migrations': 0     # Load imbalance
        }
        
        for line in output.splitlines():
            line = line.strip()
            
            # Skip unsupported events
            if '<not supported>' in line or '<not counted>' in line:
                continue
                
            try:
                parts = line.split()
                if len(parts) < 2:
                    continue
                    
                # Extract numeric value (remove commas)
                value_str = parts[0].replace(',', '')
                if not value_str.replace('.', '').isdigit():
                    continue
                value = int(float(value_str))
                
                # Map to metrics
                if 'cycles' in line and 'stalled' not in line:
                    metrics['cycles'] = value
                elif 'instructions' in line:
                    metrics['instructions'] = value
                elif 'cache-misses' in line:
                    metrics['cache_misses'] = value
                elif 'cache-references' in line:
                    metrics['cache_refs'] = value
                elif 'stalled-cycles' in line or 'idle-cycles' in line:
                    metrics['stalled_cycles'] = value
                elif 'branch-misses' in line:
                    metrics['branch_misses'] = value
                elif 'branches' in line and 'misses' not in line:
                    metrics['branches'] = value
                elif 'context-switches' in line:
                    metrics['context_switches'] = value
                elif 'cpu-migrations' in line:
                    metrics['cpu_migrations'] = value
                    
            except (ValueError, IndexError) as e:
                continue
        
        return metrics
    
    def collect_comm_metrics(self):
        """
        Collect metrics specifically relevant to MPI communication:
        Uses perf to sample system-wide activity
        """
        perf_cmd = [
            PERF_PATH, "stat", "-a",
            "-e", "cycles,instructions,cache-misses,cache-references,"
                  "stalled-cycles-frontend,stalled-cycles-backend,"
                  "branches,branch-misses,"
                  "context-switches,cpu-migrations",
            "sleep", str(SAMPLE_INTERVAL)
        ]
        
        try:
            result = subprocess.run(perf_cmd, capture_output=True, 
                                  text=True, timeout=SAMPLE_INTERVAL + 2)
            return self.parse_perf_output(result.stderr)
        except subprocess.TimeoutExpired:
            print(f"[WARN] perf timeout")
            return {}
        except Exception as e:
            print(f"[WARN] perf failed: {e}")
            return {}
    
    def classify_comm_phase(self, metrics, delta_energy):
        """
        Classify execution phase based on MPI communication characteristics
        
        Communication phases exhibit:
        1. LOW IPC (waiting for network/MPI)
        2. HIGH cache misses (packing/unpacking buffers)
        3. HIGH stalled cycles (synchronization)
        4. MODERATE power (not compute-intensive)
        5. HIGH context switches (process coordination)
        """
        
        if not metrics or metrics.get('cycles', 0) == 0:
            return "UNKNOWN", {}
        
        # Calculate derived metrics
        cycles = metrics['cycles']
        instructions = metrics['instructions']
        cache_misses = metrics['cache_misses']
        cache_refs = metrics['cache_refs']
        stalled = metrics['stalled_cycles']
        branches = metrics['branches']
        branch_misses = metrics['branch_misses']
        ctx_switches = metrics['context_switches']
        
        ipc = instructions / cycles if cycles > 0 else 0
        cache_miss_rate = cache_misses / cache_refs if cache_refs > 0 else 0
        stall_rate = stalled / cycles if cycles > 0 else 0
        branch_miss_rate = branch_misses / branches if branches > 0 else 0
        
        # Estimate MPI time based on stalls + low IPC
        # During MPI calls, CPU is often stalled waiting for network
        mpi_time_indicator = (stall_rate + (1 - ipc)) / 2
        
        # Context switches per interval (indicates MPI coordination)
        ctx_switch_rate = ctx_switches / SAMPLE_INTERVAL
        
        # Communication phase detection logic
        is_comm_phase = False
        confidence = 0.0
        reasons = []
        
        # Primary indicators
        if ipc < IPC_COMM_THRESHOLD:
            is_comm_phase = True
            confidence += 0.3
            reasons.append(f"low_ipc({ipc:.3f})")
        
        if cache_miss_rate > CACHE_MISS_THRESHOLD:
            is_comm_phase = True
            confidence += 0.25
            reasons.append(f"high_cache_miss({cache_miss_rate:.3f})")
        
        if stall_rate > STALL_CYCLE_THRESHOLD:
            is_comm_phase = True
            confidence += 0.25
            reasons.append(f"high_stalls({stall_rate:.3f})")
        
        # Secondary indicators
        if ctx_switch_rate > 100:  # High coordination
            confidence += 0.1
            reasons.append(f"high_ctx_sw({ctx_switch_rate:.0f}/s)")
        
        if mpi_time_indicator > MPI_TIME_THRESHOLD:
            confidence += 0.1
            reasons.append(f"mpi_time_est({mpi_time_indicator:.3f})")
        
        # Phase classification
        if is_comm_phase and confidence > 0.5:
            phase = "COMMUNICATION"
        elif ipc > 0.8 and cache_miss_rate < 0.2:
            phase = "COMPUTE"
            confidence = 0.0
        elif cache_miss_rate > 0.4 and ipc < 0.5:
            phase = "MEMORY_BOUND"
            confidence = 0.0
        else:
            phase = "MIXED"
            confidence *= 0.5
        
        details = {
            'ipc': ipc,
            'cache_miss_rate': cache_miss_rate,
            'stall_rate': stall_rate,
            'branch_miss_rate': branch_miss_rate,
            'mpi_time_estimate': mpi_time_indicator,
            'ctx_switches_per_sec': ctx_switch_rate,
            'cpu_migrations': metrics['cpu_migrations'],
            'confidence': confidence,
            'reasons': ','.join(reasons) if reasons else 'none'
        }
        
        return phase, details
    
    def setup_csv(self):
        """Initialize CSV with comprehensive communication metrics"""
        self.csv_file = open(LOG_FILE, "w", newline='')
        fieldnames = [
            'timestamp',
            'phase',
            'confidence',
            'ipc',
            'cache_miss_rate',
            'stall_rate',
            'mpi_time_estimate',
            'ctx_switches_per_sec',
            'cpu_migrations',
            'branch_miss_rate',
            'energy_J',
            'avg_power_W',
            'reasons'
        ]
        self.writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.writer.writeheader()
        print(f"[INFO] Logging to {LOG_FILE}")
    
    def run_monitoring(self):
        """Main monitoring loop"""
        print(f"[INFO] Starting enhanced communication phase monitoring")
        print(f"[INFO] Command: {' '.join(CMD)}")
        print(f"[INFO] Sample interval: {SAMPLE_INTERVAL}s")
        
        self.setup_csv()
        
        # Wait for miniMD to start
        print("[INFO] Waiting for miniMD to launch...")
        while not self.get_miniMD_pids():
            time.sleep(0.5)
        
        print("[INFO] miniMD detected, starting monitoring...")
        
        # Launch miniMD
        self.proc = subprocess.Popen(
            CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        self.prev_energy = self.read_energy()
        self.start_time = time.time()
        sample_count = 0
        
        try:
            while self.proc.poll() is None and self.get_miniMD_pids():
                sample_count += 1
                
                # Collect hardware metrics
                metrics = self.collect_comm_metrics()
                
                # Read energy
                energy_now = self.read_energy()
                delta_energy = max(0, energy_now - self.prev_energy) / 1e6  # µJ to J
                self.prev_energy = energy_now
                
                # Calculate average power
                avg_power = delta_energy / SAMPLE_INTERVAL if SAMPLE_INTERVAL > 0 else 0
                
                # Classify phase
                phase, details = self.classify_comm_phase(metrics, delta_energy)
                
                # Log data
                timestamp = time.time() - self.start_time
                row = {
                    'timestamp': f"{timestamp:.2f}",
                    'phase': phase,
                    'confidence': f"{details.get('confidence', 0):.3f}",
                    'ipc': f"{details.get('ipc', 0):.3f}",
                    'cache_miss_rate': f"{details.get('cache_miss_rate', 0):.3f}",
                    'stall_rate': f"{details.get('stall_rate', 0):.3f}",
                    'mpi_time_estimate': f"{details.get('mpi_time_estimate', 0):.3f}",
                    'ctx_switches_per_sec': f"{details.get('ctx_switches_per_sec', 0):.1f}",
                    'cpu_migrations': details.get('cpu_migrations', 0),
                    'branch_miss_rate': f"{details.get('branch_miss_rate', 0):.3f}",
                    'energy_J': f"{delta_energy:.4f}",
                    'avg_power_W': f"{avg_power:.2f}",
                    'reasons': details.get('reasons', 'none')
                }
                
                self.writer.writerow(row)
                self.csv_file.flush()
                
                # Print periodic status
                if sample_count % 5 == 0:
                    print(f"[{timestamp:6.1f}s] Phase: {phase:15} "
                          f"Conf: {details.get('confidence', 0):.2f} "
                          f"IPC: {details.get('ipc', 0):.2f} "
                          f"MPI%: {details.get('mpi_time_estimate', 0):.1%} "
                          f"Power: {avg_power:.1f}W")
                
                # Brief sleep
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
    """Entry point"""
    monitor = CommPhaseMonitor()
    
    # Verify perf availability
    if not os.path.exists(PERF_PATH):
        print(f"[ERROR] perf not found at {PERF_PATH}")
        return 1
    
    # Check RAPL
    if not os.path.exists(RAPL_PATH):
        print(f"[WARN] RAPL not available - energy data will be zero")
    
    try:
        monitor.run_monitoring()
    except Exception as e:
        print(f"[FATAL] {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
