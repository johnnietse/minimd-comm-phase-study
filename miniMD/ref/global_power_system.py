
import sys
import os
import time
import signal
import csv
import argparse
import random
import glob
from collections import deque
from pathlib import Path
import subprocess

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
LOOP_INTERVAL_S = 0.1  # 100ms interval
RAPL_PATH = Path("/sys/class/powercap/intel-rapl:0/energy_uj")
CPU_FREQ_PATH = "/sys/devices/system/cpu/cpu{}/cpufreq/scaling_max_freq"
TURBO_PATH = "/sys/devices/system/cpu/intel_pstate/no_turbo"
CPU_ONLINE_PATH = "/sys/devices/system/cpu/cpu{}/online"

# -----------------------------------------------------------------------------
# Thresholds (from Blueprint & Tuning)
# -----------------------------------------------------------------------------
# IO / Storage
IO_WAIT_THRESHOLD = 20.0       # > 20% Time waiting for IO
IO_LOW_UTIL_THRESHOLD = 30.0   # < 30% CPU Utilization during IO

# Communication
COMM_MPI_WAIT_THRESHOLD = 30.0 # > 30% Waiting for MPI

# Memory
MEM_CACHE_MISS_THRESHOLD = 50.0 # > 50% Cache Miss Rate

# Serial
SERIAL_CORE_LOAD_STD_DEV = 30.0 # High variance in core load (one core busy, others idle)
SERIAL_ACTIVE_CORES_MAX = 1     # Or just 1 core active

# -----------------------------------------------------------------------------
# Modules
# -----------------------------------------------------------------------------

class MonitoringModule:
    def __init__(self, simulate=False):
        self.simulate = simulate
        self.prev_rapl_energy = 0
        self.prev_proc_stat = {} # {cpu_id: (user, nice, system, idle, iowait...)}
        
        # Simulation State
        self.sim_time = 0.0
        
    def read_metrics(self):
        """
        Orchestrates reading all sensors.
        Returns a dict of metrics.
        """
        if self.simulate:
            return self._simplex_simulation()
            
        metrics = {}
        
        # 1. RAPL Power
        watts, energy = self._read_rapl()
        metrics['watts'] = watts
        self.prev_rapl_energy = energy
        
        # 2. /proc/stat (CPU Util, IO Wait, Core Variance)
        proc_stats = self._read_proc_stat()
        metrics.update(proc_stats)
        
        # 3. Perf Counters (IPC, Cache Misses - Mocked for now if perf not avail)
        perf_stats = self._read_perf_counters()
        metrics.update(perf_stats)
        
        # 4. Mock MPI Wait (In reality parsing miniMD output or using mpiP)
        # For this system, we will assume we can read a shared file or signal
        # For now, we stub it or use the simulation
        metrics['mpi_wait_pct'] = 0.0 
        
        metrics['timestamp'] = time.time()
        return metrics

    def _read_rapl(self):
        try:
            if not RAPL_PATH.exists(): return 0.0, 0
            energy = int(RAPL_PATH.read_text().strip())
            if self.prev_rapl_energy == 0: return 0.0, energy
            delta = max(0, energy - self.prev_rapl_energy)
            watts = delta / (1e6 * LOOP_INTERVAL_S)
            return watts, energy
        except:
            return 0.0, 0

    def _read_proc_stat(self):
        """
        Reads /proc/stat to get global IO wait and per-core utilization.
        """
        metrics = {
            'io_wait_pct': 0.0,
            'cpu_util_pct': 0.0,
            'core_load_std': 0.0,
            'active_cores_count': 0
        }
        try:
            with open('/proc/stat', 'r') as f:
                lines = f.readlines()
            
            core_loads = []
            
            for line in lines:
                parts = line.split()
                if not parts: continue
                
                key = parts[0]
                
                # Global 'cpu' line
                if key == 'cpu':
                    # user, nice, system, idle, iowait
                    vals = [int(x) for x in parts[1:]]
                    # Calculate IO Wait % (Simple Delta if we tracked prev, 
                    # for simplicity here taking instantaneous snapshot approach or 
                    # simplified delta calculation would be needed for production.
                    # We will assume a simplified mock for the non-simulation path 
                    # just to show structure, since we are on Windows/Simulation mostly)
                    pass 

                # Per core 'cpu0', 'cpu1'...
                if key.startswith('cpu') and len(key) > 3:
                     # Calculate per-core load
                     pass
                     
        except FileNotFoundError:
            pass
            
        return metrics

    def _read_perf_counters(self):
        # Stub for `perf stat` interaction
        return {
            'ipc': 1.0,
            'cache_miss_rate': 0.0
        }

    def _simplex_simulation(self):
        """
        Generates synthetic data cycling through 5 phases.
        Cycle: Parallel -> Communication -> Memory -> Serial -> Storage
        Duration: 30s total simulation (6s per phase)
        """
        self.sim_time += LOOP_INTERVAL_S
        metrics = {}
        
        # Define Phase Windows (6s each)
        phase_idx = int(self.sim_time / 6.0) % 5
        
        # Base Noise
        metrics['watts'] = 100 + random.uniform(-5, 5)
        metrics['ipc'] = 1.0 + random.uniform(-0.1, 0.1)
        metrics['mpi_wait_pct'] = 10.0
        metrics['io_wait_pct'] = 0.0
        metrics['cache_miss_rate'] = 5.0
        metrics['core_load_std'] = 5.0 # Low variance (all cores active)
        metrics['cpu_util_pct'] = 90.0
        
        if phase_idx == 0: # PARALLEL
            # High Load, High Watts, Low Wait
            metrics['watts'] = 200 + random.uniform(-10, 10)
            metrics['ipc'] = 1.8
            metrics['cpu_util_pct'] = 98.0
            
        elif phase_idx == 1: # COMMUNICATION
            # High MPI Wait, Low IPC
            metrics['watts'] = 120
            metrics['ipc'] = 0.4
            metrics['mpi_wait_pct'] = 80.0 # 80% Wait
            
        elif phase_idx == 2: # MEMORY
            # High Cache Misses, Med Load
            metrics['watts'] = 160
            metrics['ipc'] = 0.9
            metrics['cache_miss_rate'] = 65.0 # 65% Misses
            
        elif phase_idx == 3: # SERIAL
            # Only one core active -> High Variance
            metrics['watts'] = 80
            metrics['ipc'] = 1.5 # (single core high)
            metrics['core_load_std'] = 50.0 # High variance!
            metrics['cpu_util_pct'] = 100.0 / 32.0 # ~3% global util
            
        elif phase_idx == 4: # STORAGE
            # High IO Wait, Low Util
            metrics['watts'] = 60
            metrics['ipc'] = 0.1
            metrics['io_wait_pct'] = 60.0 # 60% IO Wait
            metrics['cpu_util_pct'] = 5.0
            
        metrics['timestamp'] = time.time()
        return metrics


class ControlModule:
    def __init__(self):
        self.current_phase = "UNKNOWN"
        self.phase_history = deque(maxlen=3)
        
        # PID State (for Communication Phase)
        self.kp = 0.005 # Scaled for % error (0-100)
        self.ki = 0.001
        self.kd = 0.0005
        self.integral = 0
        self.prev_error = 0
        
    def detect_phase(self, metrics):
        """
        Classify state into one of 5 phases.
        Priority: Storage > Serial > Comm > Memory > Parallel
        """
        phase = "PARALLEL" # Default
        
        io_wait = metrics.get('io_wait_pct', 0)
        cpu_util = metrics.get('cpu_util_pct', 0)
        mpi_wait = metrics.get('mpi_wait_pct', 0)
        core_std = metrics.get('core_load_std', 0)
        miss_rate = metrics.get('cache_miss_rate', 0)
        
        if io_wait > IO_WAIT_THRESHOLD and cpu_util < IO_LOW_UTIL_THRESHOLD:
            phase = "STORAGE"
        elif core_std > SERIAL_CORE_LOAD_STD_DEV:
            phase = "SERIAL"
        elif mpi_wait > COMM_MPI_WAIT_THRESHOLD:
            phase = "COMMUNICATION"
        elif miss_rate > MEM_CACHE_MISS_THRESHOLD:
            phase = "MEMORY"
            
        # Smoothing
        self.phase_history.append(phase)
        if len(self.phase_history) == 3 and len(set(self.phase_history)) == 1:
            self.current_phase = phase
            
        return self.current_phase

    def calculate_actions(self, phase, metrics):
        """
        Determine HW settings based on Phase.
        """
        actions = {
            'freq_scale': 1.0,
            'turbo_enabled': True,
            'active_cores_pct': 1.0,
            'strategy': phase
        }
        
        if phase == "STORAGE":
            # Min Power. CPU doing nothing.
            actions['freq_scale'] = 0.5 # Min freq
            actions['turbo_enabled'] = False
            actions['active_cores_pct'] = 0.25 # Keep minimal cores
            
        elif phase == "SERIAL":
            # Race to idle! Core 0 needs MAX speed. Others can sleep.
            # (Execution module handles specific core pinning if capable, 
            # here we request high freq but maybe fewer active cores globally)
            actions['freq_scale'] = 1.0
            actions['turbo_enabled'] = True
            actions['active_cores_pct'] = 0.1 # Very few cores needed
            
        elif phase == "COMMUNICATION":
            # PID Control for Freq
            error = metrics['mpi_wait_pct'] - COMM_MPI_WAIT_THRESHOLD
            self.integral += error
            derivative = error - self.prev_error
            output = (self.kp * error) + (self.ki * self.integral) + (self.kd * derivative)
            self.prev_error = error
            
            # Reduce freq as wait increases
            actions['freq_scale'] = max(0.5, 1.0 - output)
            actions['turbo_enabled'] = False # Latency sensitive but simple code
            
            # Deep sleep if waiting REALLY long
            if metrics['mpi_wait_pct'] > 70.0:
                 actions['active_cores_pct'] = 0.5
                 
        elif phase == "MEMORY":
            # Bandwidth bound. High freq gives diminishing returns.
            actions['freq_scale'] = 0.8 # Sweet spot
            actions['turbo_enabled'] = False
            actions['active_cores_pct'] = 1.0
            
        elif phase == "PARALLEL":
            # Max performance
            actions['freq_scale'] = 1.0
            actions['turbo_enabled'] = True
            actions['active_cores_pct'] = 1.0
            
        return actions


class ExecutionModule:
    def __init__(self, simulate=False):
        self.simulate = simulate
        self.num_cpus = os.cpu_count() or 1
        self.available_freqs = []
        if not self.simulate:
            try:
                # Read valid freqs
                with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_available_frequencies", "r") as f:
                    self.available_freqs = [int(x) for x in f.read().strip().split()]
                self.available_freqs.sort()
            except:
                self.available_freqs = [1200000, 2400000] # Mock

    def apply_actions(self, actions):
        """
        Apply global settings.
        """
        target_freq_pct = 0
        if self.available_freqs:
            # Simple mapping freq_scale (0.0-1.0) to index
            idx = int(actions['freq_scale'] * (len(self.available_freqs) - 1))
            target_freq_khz = self.available_freqs[idx]
        else:
            target_freq_khz = int(actions['freq_scale'] * 2400000)
            
        # 1. Set Frequency
        self._set_sys_freq(target_freq_khz)
        
        # 2. Set Turbo
        self._set_turbo(actions['turbo_enabled'])
        
        # 3. Set Active Cores
        num_active = max(1, int(self.num_cpus * actions['active_cores_pct']))
        self._set_active_cores(num_active)

    def _set_sys_freq(self, freq_khz):
        if self.simulate:
            # print(f"[ACTUATOR] Freq -> {freq_khz} kHz")
            return
        
        try:
            # Naive: Set all
            for i in range(self.num_cpus):
                path = CPU_FREQ_PATH.format(i)
                with open(path, 'w') as f:
                    f.write(str(freq_khz))
        except IOError:
            pass # Permission denied

    def _set_turbo(self, enabled):
        if self.simulate:
            state = "ON" if enabled else "OFF"
            # print(f"[ACTUATOR] Turbo -> {state}")
            return
            
        val = "0" if enabled else "1" # 0 = enabled (no_turbo=0)
        try:
            with open(TURBO_PATH, "w") as f:
                f.write(val)
        except IOError:
            pass

    def _set_active_cores(self, num_active):
        if self.simulate:
            # print(f"[ACTUATOR] Active Cores -> {num_active}/{self.num_cpus}")
            return
            
        try:
            # Always keep CPU 0 online. Toggle the rest.
            for i in range(1, self.num_cpus):
                state = "1" if i < num_active else "0"
                path = CPU_ONLINE_PATH.format(i)
                with open(path, 'w') as f:
                    f.write(state)
        except IOError:
            pass


class DataCollectionModule:
    def __init__(self, filepath="global_power_log.csv"):
        self.filepath = filepath
        self.header = ["Timestamp", "Phase", "Watts", "IPC", "IO_Wait", "Cache_Miss", "Core_Var", "Freq_Scale", "Turbo", "Active_Cores", "Overhead_ms"]
        
        # Create CSV if not exists
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.header)

    def log(self, metrics, phase, actions, overhead):
        row = [
            f"{metrics['timestamp']:.4f}",
            phase,
            f"{metrics.get('watts', 0):.2f}",
            f"{metrics.get('ipc', 0):.3f}",
            f"{metrics.get('io_wait_pct', 0):.2f}",
            f"{metrics.get('cache_miss_rate', 0):.2f}",
            f"{metrics.get('core_load_std', 0):.1f}",
            f"{actions['freq_scale']:.2f}",
            actions['turbo_enabled'],
            f"{actions['active_cores_pct']:.2f}",
            f"{overhead*1000:.3f}"
        ]
        
        try:
            with open(self.filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except PermissionError:
            pass 


def main():
    parser = argparse.ArgumentParser(description="Global Phase-Aware Power Optimization System")
    parser.add_argument('--simulate', action='store_true', help="Run in simulation mode")
    parser.add_argument('--duration', type=int, default=60, help="Duration to run in seconds")
    args = parser.parse_args()

    print(f"Starting Global System. Mode: {'SIMULATION' if args.simulate else 'PRODUCTION'}")
    
    monitor = MonitoringModule(simulate=args.simulate)
    control = ControlModule()
    actuator = ExecutionModule(simulate=args.simulate)
    logger = DataCollectionModule()
    
    start_time = time.time()
    
    try:
        while (time.time() - start_time) < args.duration:
            loop_start = time.time()
            
            # 1. Monitor
            metrics = monitor.read_metrics()
            
            # 2. Detect Phase
            phase = control.detect_phase(metrics)
            
            # 3. Decide Actions
            actions = control.calculate_actions(phase, metrics)
            
            # 4. Apply Actions
            actuator.apply_actions(actions)
            
            # 5. Log
            loop_end = time.time()
            overhead = loop_end - loop_start
            logger.log(metrics, phase, actions, overhead)
            
            # Print Status
            print(f"Phase: {phase:<15} | W: {metrics.get('watts', 0):>6.1f} | IO: {metrics.get('io_wait_pct',0):.2f} | Miss: {metrics.get('cache_miss_rate',0):.2f} | Act: F{actions['freq_scale']:.1f}/T{int(actions['turbo_enabled'])}/C{actions['active_cores_pct']:.1f}")
            
            # Sleep remainder
            sleep_time = max(0, LOOP_INTERVAL_S - (time.time() - loop_start))
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        print("Resetting Hardware...")
        # Reset to safe defaults
        actuator.apply_actions({'freq_scale': 1.0, 'turbo_enabled': True, 'active_cores_pct': 1.0})
        print(f"Log saved to {logger.filepath}")

if __name__ == "__main__":
    main()
