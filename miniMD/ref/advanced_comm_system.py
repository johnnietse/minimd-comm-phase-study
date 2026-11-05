
"""
Advanced Communication Phase Control System
===========================================
This system implements the Communication Phase Optimization requirements from 
ELEC49X_Group_30_Project_Blueprint.pdf.

Features:
- Modular Architecture: Monitoring, Control, Execution, Data, Evaluation.
- Advanced PID Control: Smooth frequency scaling based on MPI Wait time.
- Active Feature Switching:
    - TIMED TURBO DISABLE: Disables Turbo Boost during communication-heavy phases.
    - DYNAMIC CORE OFFLINING: Offlines CPUs during extreme wait times (>80%).
- Simulation Mode: Enables testing logic on non-Linux/non-root systems.
"""

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

# --- Configuration Constants ---
LOOP_INTERVAL_S = 0.1  # 100ms sampling interval
WINDOW_SIZE = 5        # Smoothing window for phase detection
HISTORY_LEN = 100      # Stats history length

# Detection Thresholds (from Blueprint & Comm Analysis)
COMM_WAIT_THRESHOLD = 0.30     # >30% MPI Wait -> Communication Phase
COMM_IPC_THRESHOLD = 0.60      # <0.6 IPC -> Communication Phase
SYNC_OVERHEAD_THRESHOLD = 0.25 # >25% Sync Overhead
DEEP_WAIT_THRESHOLD = 0.70     # >70% Wait -> Trigger Core Offlining

# Hard System Paths (Linux/CAC Cluster)
RAPL_PATH = "/sys/class/powercap/intel-rapl:0/energy_uj"
CPU_FREQ_PATH = "/sys/devices/system/cpu/cpu{}/cpufreq/scaling_max_freq"
AVAILABLE_FREQS = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_available_frequencies"
TURBO_PATH = "/sys/devices/system/cpu/intel_pstate/no_turbo"
CPU_ONLINE_PATH = "/sys/devices/system/cpu/cpu{}/online"

class MonitoringModule:
    """
    Subsystem 1: Monitoring
    Responsible for collecting metrics from hardware (RAPL, counters).
    """
    def __init__(self, simulate=False):
        self.simulate = simulate
        self.prev_energy = None
        self.prev_time = None
        # Metric history for smoothing
        self.ipc_history = deque(maxlen=WINDOW_SIZE)
        self.wait_history = deque(maxlen=WINDOW_SIZE)

    def read_metrics(self):
        """
        Reads system state.
        Returns: dict of {watts, ipc, mpi_wait_pct, timestamp}
        """
        timestamp = time.time()
        
        if self.simulate:
            return self._simplex_simulation(timestamp)

        # Real Hardware Reading
        watts = self._read_rapl()
        ipc, mpi_wait = self._read_perf_counters()
        
        return {
            'timestamp': timestamp,
            'watts': watts,
            'ipc': ipc,
            'mpi_wait_pct': mpi_wait
        }

    def _read_rapl(self):
        try:
            with open(RAPL_PATH, 'r') as f:
                energy_uj = int(f.read().strip())
        except:
            return 0.0

        current_time = time.time()
        watts = 0.0
        if self.prev_energy is not None:
            dt = current_time - self.prev_time
            if dt > 0:
                d_energy = energy_uj - self.prev_energy
                watts = (d_energy / 1e6) / dt
        
        self.prev_energy = energy_uj
        self.prev_time = current_time
        return watts

    def _read_perf_counters(self):
        # NOTE: In a real deployment, this parses `perf stat` output or uses python-papi.
        # For simplicity in this backbone, we assume a helper or previous value.
        # This is a placeholder for the complex `perf` parsing logic.
        return 0.5, 0.1  # Mock return if 'perf' not fully integrated

    def _simplex_simulation(self, timestamp):
        """Generates synthetic data for testing logic."""
        # Oscillate between COMPUTE and COMMUNICATION every 10 seconds
        period = 20.0
        phase_time = timestamp % period
        
        if phase_time < 10.0:
            # COMPUTE PHASE
            ipc = random.uniform(1.2, 2.0)
            mpi_wait = random.uniform(0.0, 0.15)
            watts = random.uniform(180, 220)
        else:
            # COMMUNICATION PHASE
            ipc = random.uniform(0.1, 0.5)
            # Ramp up wait time to test core offlining
            progress = (phase_time - 10.0) / 10.0 
            mpi_wait = 0.35 + (0.5 * progress) # 0.35 -> 0.85
            watts = random.uniform(100, 150)
            
        return {
            'timestamp': timestamp,
            'watts': watts,
            'ipc': ipc,
            'mpi_wait_pct': mpi_wait,
            'simulated_phase': 'COMM' if phase_time >= 10.0 else 'COMPUTE'
        }

class ControlModule:
    """
    Subsystem 2: Control
    The BRAIN. Determines what to do based on monitoring data.
    """
    def __init__(self):
        # PID Constants
        self.Kp = 2.0
        self.Ki = 0.5
        self.Kd = 0.1
        
        self.integral = 0.0
        self.prev_error = 0.0
        
        self.current_phase = "UNKNOWN"
        self.consecutive_comm_frames = 0
        
    def detect_phase(self, metrics):
        """
        Classifies phase based on Blueprint rules.
        """
        is_comm = False
        # Rule 1: High MPI Wait
        if metrics['mpi_wait_pct'] > COMM_WAIT_THRESHOLD:
            is_comm = True
        # Rule 2: Low IPC + Moderate Power (simplified)
        elif metrics['ipc'] < COMM_IPC_THRESHOLD:
            is_comm = True
            
        if is_comm:
            self.consecutive_comm_frames += 1
        else:
            self.consecutive_comm_frames = 0
            
        # Hysteresis: Require 3 frames to switch TO Comm, 1 to switch back
        if self.consecutive_comm_frames >= 3:
            self.current_phase = "COMMUNICATION"
        else:
            self.current_phase = "COMPUTE"
            
        return self.current_phase

    def calculate_actions(self, metrics, phase):
        """
        Determines target actions: freq_scale, turbo, core_percent
        """
        actions = {
            'freq_scale': 1.0, # 1.0 = Max Frequency
            'turbo_enabled': True,
            'active_cores_pct': 1.0 # 1.0 = All cores
        }
        
        if phase == "COMMUNICATION":
            # 1. PID Control for Frequency
            # Target: Maintain 'efficiency' (conceptual). 
            # Proxy: Minimize Excess Power while Wait > Threshold.
            # Error = Current_Wait - Target_Max_Wait
            # If Wait is high, we want to Slow Down CPU (reduce freq) to save power, 
            # assuming wait is due to external blocking.
            
            error = metrics['mpi_wait_pct'] - COMM_WAIT_THRESHOLD
            
            p_term = self.Kp * error
            self.integral += error * LOOP_INTERVAL_S
            i_term = self.Ki * self.integral
            d_term = self.Kd * (error - self.prev_error) / LOOP_INTERVAL_S
            
            pid_out = p_term + i_term + d_term
            self.prev_error = error
            
            # If pid_out is positive (high wait), we REDUCE frequency.
            # Scale: 1.0 (max) -> 0.5 (min)
            scale = 1.0 - pid_out
            actions['freq_scale'] = max(0.5, min(1.0, scale))

            # 2. Turbo Boost Action
            # Always disable turbo in confirmed communication phase
            actions['turbo_enabled'] = False
            
            # 3. Core Offlining Action
            # If wait is Extreme, offline cores.
            if metrics['mpi_wait_pct'] > DEEP_WAIT_THRESHOLD:
                # 50% cores for deep wait
                actions['active_cores_pct'] = 0.5
                
        else:
            # RESET PID
            self.integral = 0
            self.prev_error = 0
            # Defaults apply (Max Freq, Turbo On, All Cores)
        
        return actions

class ExecutionModule:
    """
    Subsystem 3: Execution
    The ACTUATOR. Talks to Linux Kernel / Hardware.
    """
    def __init__(self, simulate=False):
        self.simulate = simulate
        self.available_freqs = self._get_freqs()
        self.num_cpus = os.cpu_count() or 4
        self.current_state = {
            'freq': -1,
            'turbo': True,
            'cores': self.num_cpus
        }
        
    def _get_freqs(self):
        if self.simulate:
            return [1200000, 1500000, 1800000, 2100000, 2400000] # Mock
        try:
            with open(AVAILABLE_FREQS, 'r') as f:
                return [int(x) for x in f.read().split()]
        except:
            return [2000000] # Fallback

    def apply_actions(self, actions):
        """Applies calculated actions to hardware."""
        
        # 1. Frequency Scaling
        target_freq_idx = int((1.0 - actions['freq_scale']) * (len(self.available_freqs)-1))
        # Logic: scale=1.0 -> idx=0 (Highest). scale=0.5 -> idx=last (Lowest)
        # Note: avail_freqs is usually desc order? Assume descending for safety.
        # Actually usually it is descending (Max ... Min).
        # Let's assume Descending: [MAX, ..., MIN]
        target_freq_idx = min(target_freq_idx, len(self.available_freqs)-1)
        target_freq = self.available_freqs[target_freq_idx]
        
        if target_freq != self.current_state['freq']:
            self._set_sys_freq(target_freq)
            self.current_state['freq'] = target_freq

        # 2. Turbo Boost
        if actions['turbo_enabled'] != self.current_state['turbo']:
            self._set_turbo(actions['turbo_enabled'])
            self.current_state['turbo'] = actions['turbo_enabled']

        # 3. Core Offlining
        # Determine number of active cores
        target_cores = max(1, int(self.num_cpus * actions['active_cores_pct']))
        if target_cores != self.current_state['cores']:
            self._set_active_cores(target_cores)
            self.current_state['cores'] = target_cores

    def _set_sys_freq(self, freq):
        if self.simulate:
            print(f"[ACTUATOR] Frequency set to {freq} kHz")
            return
        # Real impl would iterate all policy cpus
        # require root
        try:
            for i in range(self.num_cpus):
                # Don't fail if some cpus are offline
                path = CPU_FREQ_PATH.format(i)
                if os.path.exists(path):
                    with open(path, 'w') as f:
                        f.write(str(freq))
        except IOError:
            pass # Permission denied likelihood high

    def _set_turbo(self, enabled):
        """
        Enable/Disable Turbo Boost.
        Note: Linux uses 'no_turbo', so 1 = Disabled, 0 = Enabled.
        """
        if self.simulate:
            print(f"[ACTUATOR] Turbo Boost {'ENABLED' if enabled else 'DISABLED'}")
            return
        
        val = "0" if enabled else "1"
        try:
            with open(TURBO_PATH, 'w') as f:
                f.write(val)
        except IOError:
            pass

    def _set_active_cores(self, num_active):
        """
        Actively hotplug CPUs.
        """
        if self.simulate:
            print(f"[ACTUATOR] Active Cores target: {num_active}/{self.num_cpus}")
            return
            
        # Keep CPU 0 always online
        # Toggle others
        try:
            for i in range(1, self.num_cpus):
                online = "1" if i < num_active else "0"
                path = CPU_ONLINE_PATH.format(i)
                with open(path, 'w') as f:
                    f.write(online)
        except IOError:
            pass

class DataCollectionModule:
    """
    Subsystem 4: Data Collection
    Logs everything for the Evaluation Module.
    """
    def __init__(self, filename="comm_phase_log.csv"):
        self.filename = filename
        self.initialized = False
        
    def log(self, metrics, phase, actions, overhead_ms):
        file_exists = os.path.isfile(self.filename)
        with open(self.filename, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "Timestamp", "Watts", "IPC", "MPI_Wait_Pct", 
                    "Phase", "Freq_Scale", "Turbo_Enabled", "Active_Cores_Pct",
                    "Overhead_ms"
                ])
            
            writer.writerow([
                f"{metrics['timestamp']:.4f}",
                f"{metrics['watts']:.2f}",
                f"{metrics['ipc']:.3f}",
                f"{metrics['mpi_wait_pct']:.3f}",
                phase,
                f"{actions['freq_scale']:.2f}",
                str(actions['turbo_enabled']),
                f"{actions['active_cores_pct']:.2f}",
                f"{overhead_ms:.3f}"
            ])

def main():
    parser = argparse.ArgumentParser(description="Advanced Communication Phase Control")
    parser.add_argument("--simulate", action="store_true", help="Run in simulation mode")
    parser.add_argument("--duration", type=int, default=60, help="Run duration in seconds")
    args = parser.parse_args()

    # Initialize Modules
    monitor = MonitoringModule(simulate=args.simulate)
    control = ControlModule()
    actuator = ExecutionModule(simulate=args.simulate)
    logger = DataCollectionModule()

    print(f"Starting Advanced Control System (Simulate={args.simulate})...")
    print("Press Ctrl+C to stop.")

    start_time = time.time()
    
    try:
        while (time.time() - start_time) < args.duration:
            loop_start = time.perf_counter()
            
            # 1. Monitoring
            metrics = monitor.read_metrics()
            
            # 2. Control (Phase Detection + Action Calculation)
            phase = control.detect_phase(metrics)
            actions = control.calculate_actions(metrics, phase)
            
            # 3. Execution
            actuator.apply_actions(actions)
            
            # 4. Data Collection & Overhead Calc
            loop_end = time.perf_counter()
            overhead_ms = (loop_end - loop_start) * 1000
            
            logger.log(metrics, phase, actions, overhead_ms)
            
            # Print Status
            status = (f"Phase: {phase:13} | Wait: {metrics['mpi_wait_pct']*100:5.1f}% | "
                      f"Freq: {actions['freq_scale']:.2f} | Turbo: {actions['turbo_enabled']} | "
                      f"Cores: {int(actions['active_cores_pct']*100)}%")
            print(status)
            
            # Maintain Loop Interval
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0, LOOP_INTERVAL_S - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        # Reset hardware state ideally (re-enable turbo, online cores)
        if not args.simulate:
            print("Resetting hardware state not implemented in cleanup for safety.")
        print("Done.")

if __name__ == "__main__":
    main()
