#!/usr/bin/env python3
"""
Advanced Communication Phase Monitor with PID Control
Implements adaptive power management during MPI communication phases
with intelligent feature toggling and predictive optimization.
"""

import time
import subprocess
import csv
import json
import os
import signal
import sys
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import threading
import numpy as np

# =====================================================
#                    CONFIGURATION
# =====================================================

@dataclass
class Config:
    """System configuration parameters"""
    # Monitoring intervals
    PERF_INTERVAL: float = 0.5  # 500ms for communication phases (faster than compute)
    PREDICTION_WINDOW: int = 10  # samples for prediction
    
    # PID Controller parameters
    KP: float = 0.8  # Proportional gain
    KI: float = 0.3  # Integral gain
    KD: float = 0.15  # Derivative gain
    
    # Phase detection thresholds
    COMM_IPC_THRESHOLD: float = 0.4  # Low IPC indicates communication wait
    COMM_POWER_THRESHOLD: float = 3.0  # Watts above idle
    MPI_TIME_THRESHOLD: float = 0.25  # 25% of time in MPI calls
    
    # Power management constraints
    MIN_FREQ_MHZ: int = 1200  # Minimum CPU frequency
    MAX_FREQ_MHZ: int = 2000  # Maximum CPU frequency
    IDLE_FREQ_MHZ: int = 1400  # Frequency during comm wait
    
    # Energy targets
    ENERGY_TARGET_REDUCTION: float = 0.12  # 12% target reduction
    MAX_PERFORMANCE_IMPACT: float = 0.03  # 3% max slowdown
    
    # File paths
    CSV_PATH: str = "comm_phase_monitor.csv"
    STATE_PATH: str = "comm_phase_state.json"
    RAPL_PKG: str = "/sys/class/powercap/intel-rapl:0/energy_uj"
    PERF: str = "/cvmfs/soft.computecanada.ca/gentoo/2023/x86-64-v3/usr/bin/perf"

CONFIG = Config()

# =====================================================
#                 FEATURE CONTROL SYSTEM
# =====================================================

class FeatureController:
    """Manages CPU features that can be toggled for power savings"""
    
    def __init__(self):
        self.features = {
            'turbo_boost': {'enabled': True, 'power_cost': 15.0},
            'hyperthreading': {'enabled': True, 'power_cost': 8.0},
            'prefetchers': {'enabled': True, 'power_cost': 5.0},
            'c_states': {'enabled': True, 'power_cost': -3.0},  # negative = saves power
            'uncore_freq': {'enabled': True, 'power_cost': 10.0}
        }
        self.disabled_features = set()
        
    def can_disable(self, feature: str) -> bool:
        """Check if feature can be safely disabled during comm phase"""
        # Turbo and prefetchers safe to disable during comm wait
        safe_to_disable = {'turbo_boost', 'prefetchers', 'uncore_freq'}
        return feature in safe_to_disable and feature not in self.disabled_features
    
    def disable_feature(self, feature: str) -> bool:
        """Disable a CPU feature to save power"""
        if not self.can_disable(feature):
            return False
            
        try:
            if feature == 'turbo_boost':
                subprocess.run(['sudo', 'sh', '-c', 
                    'echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo'],
                    check=True, capture_output=True)
            elif feature == 'prefetchers':
                # Disable hardware prefetchers via MSR (requires msr-tools)
                subprocess.run(['sudo', 'wrmsr', '-a', '0x1a4', '0xf'],
                    check=True, capture_output=True)
            elif feature == 'uncore_freq':
                # Reduce uncore (cache/memory controller) frequency
                subprocess.run(['sudo', 'sh', '-c',
                    'echo 1200000 > /sys/devices/system/cpu/intel_uncore_frequency/package_00_die_00/max_freq_khz'],
                    check=True, capture_output=True)
            
            self.disabled_features.add(feature)
            print(f"[FEATURE] Disabled {feature} (saving ~{self.features[feature]['power_cost']}W)")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to disable {feature}: {e}")
            return False
    
    def enable_feature(self, feature: str) -> bool:
        """Re-enable a CPU feature"""
        if feature not in self.disabled_features:
            return False
            
        try:
            if feature == 'turbo_boost':
                subprocess.run(['sudo', 'sh', '-c',
                    'echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo'],
                    check=True, capture_output=True)
            elif feature == 'prefetchers':
                subprocess.run(['sudo', 'wrmsr', '-a', '0x1a4', '0x0'],
                    check=True, capture_output=True)
            elif feature == 'uncore_freq':
                subprocess.run(['sudo', 'sh', '-c',
                    'echo 2000000 > /sys/devices/system/cpu/intel_uncore_frequency/package_00_die_00/max_freq_khz'],
                    check=True, capture_output=True)
            
            self.disabled_features.remove(feature)
            print(f"[FEATURE] Enabled {feature}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to enable {feature}: {e}")
            return False
    
    def restore_all(self):
        """Restore all features to default state"""
        for feature in list(self.disabled_features):
            self.enable_feature(feature)

# =====================================================
#                  PID CONTROLLER
# =====================================================

class PIDController:
    """PID controller for adaptive power management"""
    
    def __init__(self, kp: float, ki: float, kd: float, setpoint: float):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.setpoint = setpoint  # Target power reduction
        
        self.integral = 0.0
        self.previous_error = 0.0
        self.output_limits = (CONFIG.MIN_FREQ_MHZ, CONFIG.MAX_FREQ_MHZ)
        
    def update(self, current_value: float, dt: float) -> float:
        """
        Calculate PID output
        current_value: current power savings ratio (0-1)
        dt: time delta since last update
        Returns: frequency adjustment (MHz)
        """
        error = self.setpoint - current_value
        
        # Proportional term
        p_term = self.kp * error
        
        # Integral term with anti-windup
        self.integral += error * dt
        self.integral = np.clip(self.integral, -100, 100)  # Prevent windup
        i_term = self.ki * self.integral
        
        # Derivative term
        d_term = self.kd * (error - self.previous_error) / dt if dt > 0 else 0
        self.previous_error = error
        
        # Calculate output (frequency adjustment)
        output = p_term + i_term + d_term
        
        # Convert to frequency (scale appropriately)
        freq_adjustment = output * (CONFIG.MAX_FREQ_MHZ - CONFIG.MIN_FREQ_MHZ)
        target_freq = CONFIG.IDLE_FREQ_MHZ + freq_adjustment
        
        # Clamp to limits
        target_freq = np.clip(target_freq, *self.output_limits)
        
        return int(target_freq)
    
    def reset(self):
        """Reset controller state"""
        self.integral = 0.0
        self.previous_error = 0.0

# =====================================================
#              COMMUNICATION PATTERN ANALYZER
# =====================================================

class CommPatternAnalyzer:
    """Analyzes MPI communication patterns for predictive optimization"""
    
    def __init__(self, window_size: int = 20):
        self.window_size = window_size
        self.comm_durations = deque(maxlen=window_size)
        self.comm_intervals = deque(maxlen=window_size)
        self.last_comm_end = 0
        
    def record_comm_phase(self, duration: float, timestamp: float):
        """Record communication phase timing"""
        self.comm_durations.append(duration)
        if self.last_comm_end > 0:
            interval = timestamp - self.last_comm_end
            self.comm_intervals.append(interval)
        self.last_comm_end = timestamp + duration
    
    def predict_next_comm(self) -> Optional[Tuple[float, float]]:
        """
        Predict next communication phase
        Returns: (predicted_time, predicted_duration) or None
        """
        if len(self.comm_intervals) < 5:
            return None
        
        # Simple exponential smoothing for prediction
        alpha = 0.3
        pred_interval = self.comm_intervals[-1]
        for interval in list(self.comm_intervals)[-5:]:
            pred_interval = alpha * interval + (1 - alpha) * pred_interval
        
        pred_duration = np.mean(list(self.comm_durations)[-5:])
        pred_time = time.time() + pred_interval
        
        return (pred_time, pred_duration)
    
    def get_statistics(self) -> Dict:
        """Get communication pattern statistics"""
        if not self.comm_durations:
            return {}
        
        return {
            'avg_duration': np.mean(self.comm_durations),
            'std_duration': np.std(self.comm_durations),
            'avg_interval': np.mean(self.comm_intervals) if self.comm_intervals else 0,
            'regularity': 1.0 / (1.0 + np.std(self.comm_intervals)) if self.comm_intervals else 0
        }

# =====================================================
#                  POWER MONITOR
# =====================================================

class PowerMonitor:
    """Advanced power monitoring with RAPL"""
    
    def __init__(self):
        self.rapl_path = Path(CONFIG.RAPL_PKG)
        self.prev_energy = self.read_energy()
        self.baseline_power = None
        self.idle_power = None
        
    def read_energy(self) -> int:
        """Read RAPL energy counter (microjoules)"""
        try:
            return int(self.rapl_path.read_text().strip())
        except Exception as e:
            print(f"[ERROR] Failed to read RAPL: {e}")
            return 0
    
    def get_power(self, dt: float) -> float:
        """Calculate instantaneous power (Watts)"""
        current_energy = self.read_energy()
        delta = max(0, current_energy - self.prev_energy)
        self.prev_energy = current_energy
        
        if dt > 0:
            return (delta / 1e6) / dt  # Convert μJ to J, then to W
        return 0.0
    
    def calibrate_baseline(self, samples: int = 10):
        """Calibrate baseline power consumption"""
        powers = []
        for _ in range(samples):
            time.sleep(0.5)
            power = self.get_power(0.5)
            if power > 0:
                powers.append(power)
        
        if powers:
            self.baseline_power = np.mean(powers)
            self.idle_power = np.percentile(powers, 25)  # 25th percentile as idle
            print(f"[CALIBRATION] Baseline power: {self.baseline_power:.2f}W, Idle: {self.idle_power:.2f}W")

# =====================================================
#                 PERFORMANCE MONITOR
# =====================================================

class PerformanceMonitor:
    """Monitor CPU performance counters"""
    
    def __init__(self):
        self.perf_cmd = CONFIG.PERF
        
    def sample_metrics(self, duration: float) -> Dict:
        """Sample performance counters"""
        cmd = [
            self.perf_cmd, 'stat', '-a', '-e',
            'cycles,instructions,cache-references,cache-misses,'
            'context-switches,cpu-migrations',
            'sleep', str(duration)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, stderr=subprocess.STDOUT)
        
        metrics = {
            'cycles': 0,
            'instructions': 0,
            'cache_refs': 0,
            'cache_misses': 0,
            'context_switches': 0,
            'cpu_migrations': 0
        }
        
        for line in result.stdout.splitlines():
            if 'cycles' in line and 'stalled' not in line:
                metrics['cycles'] = self._extract_value(line)
            elif 'instructions' in line:
                metrics['instructions'] = self._extract_value(line)
            elif 'cache-references' in line:
                metrics['cache_refs'] = self._extract_value(line)
            elif 'cache-misses' in line:
                metrics['cache_misses'] = self._extract_value(line)
            elif 'context-switches' in line:
                metrics['context_switches'] = self._extract_value(line)
            elif 'cpu-migrations' in line:
                metrics['cpu_migrations'] = self._extract_value(line)
        
        # Calculate derived metrics
        metrics['ipc'] = metrics['instructions'] / metrics['cycles'] if metrics['cycles'] > 0 else 0
        metrics['cache_miss_rate'] = metrics['cache_misses'] / metrics['cache_refs'] if metrics['cache_refs'] > 0 else 0
        
        return metrics
    
    def _extract_value(self, line: str) -> int:
        """Extract numeric value from perf stat line"""
        token = line.split()[0].replace(',', '')
        if token.startswith('<') or not token.isdigit():
            return 0
        return int(token)

# =====================================================
#                   MPI MONITOR
# =====================================================

class MPIMonitor:
    """Monitor MPI process communication activity"""
    
    def __init__(self):
        self.mpi_pids = []
        self.prev_voluntary_switches = {}
        self.prev_nonvoluntary_switches = {}
        
    def get_mpi_pids(self) -> List[int]:
        """Get PIDs of MPI processes (miniMD)"""
        try:
            result = subprocess.run(['pgrep', '-f', 'miniMD'], 
                                    capture_output=True, text=True, check=True)
            pids = [int(pid) for pid in result.stdout.strip().split()]
            self.mpi_pids = pids
            return pids
        except subprocess.CalledProcessError:
            return []
    
    def get_comm_activity(self) -> Dict:
        """
        Estimate communication activity via context switches
        High voluntary switches = waiting for I/O/network
        """
        if not self.mpi_pids:
            self.get_mpi_pids()
        
        total_vol_switches = 0
        total_nonvol_switches = 0
        active_pids = 0
        
        for pid in self.mpi_pids:
            try:
                with open(f'/proc/{pid}/status', 'r') as f:
                    status = f.read()
                
                vol = 0
                nonvol = 0
                for line in status.splitlines():
                    if line.startswith('voluntary_ctxt_switches:'):
                        vol = int(line.split()[1])
                    elif line.startswith('nonvoluntary_ctxt_switches:'):
                        nonvol = int(line.split()[1])
                
                # Calculate delta since last reading
                vol_delta = vol - self.prev_voluntary_switches.get(pid, vol)
                nonvol_delta = nonvol - self.prev_nonvoluntary_switches.get(pid, nonvol)
                
                self.prev_voluntary_switches[pid] = vol
                self.prev_nonvoluntary_switches[pid] = nonvol
                
                total_vol_switches += max(0, vol_delta)
                total_nonvol_switches += max(0, nonvol_delta)
                active_pids += 1
                
            except FileNotFoundError:
                continue
        
        if active_pids == 0:
            return {'comm_intensity': 0, 'wait_ratio': 0, 'active_ranks': 0}
        
        # Voluntary switches indicate waiting (I/O, MPI blocking calls)
        total_switches = total_vol_switches + total_nonvol_switches
        wait_ratio = total_vol_switches / total_switches if total_switches > 0 else 0
        
        return {
            'comm_intensity': total_switches / active_pids,
            'wait_ratio': wait_ratio,
            'active_ranks': active_pids
        }

# =====================================================
#                 FREQUENCY CONTROLLER
# =====================================================

class FrequencyController:
    """Control CPU frequency scaling"""
    
    def __init__(self):
        self.current_freq = self.get_current_freq()
        self.num_cpus = os.cpu_count()
        
    def get_current_freq(self) -> int:
        """Get current CPU frequency (MHz)"""
        try:
            with open('/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq', 'r') as f:
                return int(f.read().strip()) // 1000  # Convert kHz to MHz
        except Exception:
            return CONFIG.MAX_FREQ_MHZ
    
    def set_frequency(self, freq_mhz: int, cores: Optional[List[int]] = None):
        """
        Set CPU frequency
        cores: list of CPU IDs to adjust (None = all cores)
        """
        freq_khz = freq_mhz * 1000
        
        if cores is None:
            cores = range(self.num_cpus)
        
        for cpu in cores:
            try:
                # Set max frequency
                subprocess.run(['sudo', 'sh', '-c',
                    f'echo {freq_khz} > /sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_max_freq'],
                    check=True, capture_output=True)
                # Set min frequency (to force the frequency)
                subprocess.run(['sudo', 'sh', '-c',
                    f'echo {freq_khz} > /sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_min_freq'],
                    check=True, capture_output=True)
            except Exception as e:
                print(f"[ERROR] Failed to set frequency for CPU{cpu}: {e}")
                return False
        
        self.current_freq = freq_mhz
        return True
    
    def reset_frequency(self):
        """Reset to default frequency"""
        self.set_frequency(CONFIG.MAX_FREQ_MHZ)

# =====================================================
#           MAIN COMMUNICATION PHASE MONITOR
# =====================================================

class CommunicationPhaseMonitor:
    """Main monitoring system with PID control and adaptive optimization"""
    
    def __init__(self):
        self.power_monitor = PowerMonitor()
        self.perf_monitor = PerformanceMonitor()
        self.mpi_monitor = MPIMonitor()
        self.freq_controller = FrequencyController()
        self.feature_controller = FeatureController()
        self.pattern_analyzer = CommPatternAnalyzer()
        
        # PID controller targeting energy reduction
        self.pid = PIDController(
            kp=CONFIG.KP,
            ki=CONFIG.KI,
            kd=CONFIG.KD,
            setpoint=CONFIG.ENERGY_TARGET_REDUCTION
        )
        
        # State tracking
        self.current_phase = "UNKNOWN"
        self.phase_start_time = time.time()
        self.energy_saved = 0.0
        self.baseline_energy = 0.0
        self.phase_history = deque(maxlen=100)
        
        # CSV logging
        self.csv_file = None
        self.csv_writer = None
        
        # Performance tracking
        self.performance_history = deque(maxlen=50)
        self.baseline_performance = None
        
    def initialize(self):
        """Initialize monitoring system"""
        print("[INIT] Communication Phase Monitor starting...")
        
        # Calibrate baseline power
        print("[INIT] Calibrating baseline power...")
        self.power_monitor.calibrate_baseline()
        
        # Wait for MPI processes
        print("[INIT] Waiting for miniMD processes...")
        while not self.mpi_monitor.get_mpi_pids():
            time.sleep(1)
        print(f"[INIT] Found {len(self.mpi_monitor.mpi_pids)} MPI ranks")
        
        # Initialize CSV logging
        self.csv_file = open(CONFIG.CSV_PATH, 'w', newline='')
        fieldnames = [
            'timestamp', 'phase', 'power_watts', 'ipc', 'cache_miss_rate',
            'comm_intensity', 'wait_ratio', 'cpu_freq_mhz', 'energy_saved_pct',
            'performance_impact_pct', 'pid_output', 'features_disabled',
            'prediction_accuracy'
        ]
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.csv_writer.writeheader()
        
        print("[INIT] Initialization complete\n")
    
    def detect_phase(self, metrics: Dict, mpi_activity: Dict) -> str:
        """
        Detect if currently in communication phase
        """
        ipc = metrics.get('ipc', 1.0)
        comm_intensity = mpi_activity.get('comm_intensity', 0)
        wait_ratio = mpi_activity.get('wait_ratio', 0)
        
        # Communication phase indicators:
        # - Low IPC (CPU waiting)
        # - High context switching (MPI blocking calls)
        # - High voluntary wait ratio
        
        is_comm_phase = (
            ipc < CONFIG.COMM_IPC_THRESHOLD and
            wait_ratio > 0.6 and
            comm_intensity > 50  # arbitrary threshold
        )
        
        return "COMMUNICATION" if is_comm_phase else "COMPUTE"
    
    def optimize_for_comm_phase(self, metrics: Dict, power: float, dt: float) -> Dict:
        """
        Apply PID-controlled optimization during communication phase
        Returns optimization actions taken
        """
        actions = {
            'freq_adjusted': False,
            'features_disabled': [],
            'target_freq': self.freq_controller.current_freq
        }
        
        # Calculate current energy savings
        if self.power_monitor.baseline_power:
            current_savings = 1.0 - (power / self.power_monitor.baseline_power)
        else:
            current_savings = 0.0
        
        # PID update
        target_freq = self.pid.update(current_savings, dt)
        
        # Apply frequency adjustment
        if abs(target_freq - self.freq_controller.current_freq) > 50:  # 50 MHz deadband
            self.freq_controller.set_frequency(target_freq)
            actions['freq_adjusted'] = True
            actions['target_freq'] = target_freq
        
        # Feature toggling based on power savings progress
        if current_savings < CONFIG.ENERGY_TARGET_REDUCTION * 0.5:
            # Not meeting target - disable more features
            for feature in ['turbo_boost', 'prefetchers', 'uncore_freq']:
                if self.feature_controller.can_disable(feature):
                    if self.feature_controller.disable_feature(feature):
                        actions['features_disabled'].append(feature)
                        break  # Disable one at a time
        
        elif current_savings > CONFIG.ENERGY_TARGET_REDUCTION * 1.2:
            # Exceeding target - can re-enable features
            if self.feature_controller.disabled_features:
                feature = list(self.feature_controller.disabled_features)[0]
                self.feature_controller.enable_feature(feature)
        
        return actions
    
    def restore_for_compute_phase(self):
        """Restore full performance for compute phase"""
        self.freq_controller.reset_frequency()
        self.feature_controller.restore_all()
        self.pid.reset()
    
    def calculate_performance_impact(self) -> float:
        """
        Calculate performance impact from power optimization
        Returns percentage slowdown (negative = speedup)
        """
        if not self.performance_history or self.baseline_performance is None:
            return 0.0
        
        current_perf = np.mean(list(self.performance_history)[-10:])
        impact = (current_perf - self.baseline_performance) / self.baseline_performance
        return impact * 100  # Convert to percentage
    
    def log_metrics(self, timestamp: float, phase: str, metrics: Dict, 
                    mpi_activity: Dict, power: float, actions: Dict):
        """Log metrics to CSV"""
        perf_impact = self.calculate_performance_impact()
        energy_saved_pct = (1.0 - power / self.power_monitor.baseline_power) * 100 if self.power_monitor.baseline_power else 0
        
        row = {
            'timestamp': f"{timestamp:.2f}",
            'phase': phase,
            'power_watts': f"{power:.2f}",
            'ipc': f"{metrics.get('ipc', 0):.3f}",
            'cache_miss_rate': f"{metrics.get('cache_miss_rate', 0):.3f}",
            'comm_intensity': f"{mpi_activity.get('comm_intensity', 0):.1f}",
            'wait_ratio': f"{mpi_activity.get('wait_ratio', 0):.3f}",
            'cpu_freq_mhz': f"{actions.get('target_freq', 0)}",
            'energy_saved_pct': f"{energy_saved_pct:.2f}",
            'performance_impact_pct': f"{perf_impact:.2f}",
            'pid_output': f"{actions.get('target_freq', 0)}",
            'features_disabled': ','.join(self.feature_controller.disabled_features),
            'prediction_accuracy': "N/A"
        }
        
        self.csv_writer.writerow(row)
        self.csv_file.flush()
    
    def run(self):
        """Main monitoring loop"""
        self.initialize()
        
        start_time = time.time()
        last_time = start_time
        iteration = 0
        
        try:
            while True:
                loop_start = time.time()
                
                # Check if miniMD still running
                if not self.mpi_monitor.get_mpi_pids():
                    print("\n[STOP] miniMD finished. Exiting monitor.")
                    break
                
                # Sample metrics
                perf_metrics = self.perf_monitor.sample_metrics(CONFIG.PERF_INTERVAL)
                mpi_activity = self.mpi_monitor.get_comm_activity()
                
                current_time = time.time()
                dt = current_time - last_time
                power = self.power_monitor.get_power(dt)
                
                # Detect phase
                phase = self.detect_phase(perf_metrics, mpi_activity)
                
                # Handle phase transitions
                if phase != self.current_phase:
                    phase_duration = current_time - self.phase_start_time
                    print(f"\n[PHASE TRANSITION] {self.current_phase} -> {phase} (duration: {phase_duration:.2f}s)")
                    
                    if phase == "COMMUNICATION":
                        # Record pattern for prediction
                        self.pattern_analyzer.record_comm_phase(phase_duration, current_time)
                        print(f"[COMM PATTERN] {self.pattern_analyzer.get_statistics()}")
                    
                    self.current_phase = phase
                    self.phase_start_time = current_time
                    self.phase_history.append({
                        'phase': phase,
                        'timestamp': current_time,
                        'duration': phase_duration
                    })
                
                # Apply optimization
                actions = {}
                if phase == "COMMUNICATION":
                    actions = self.optimize_for_comm_phase(perf_metrics, power, dt)
                    if actions['freq_adjusted'] or actions['features_disabled']:
                        print(f"[OPTIMIZE] Freq: {actions['target_freq']}MHz, Disabled: {actions['features_disabled']}")
                elif phase == "COMPUTE" and self.current_phase == "COMMUNICATION":
                    # Transitioning out of comm phase
                    self.restore_for_compute_phase()
                    print("[RESTORE] Full performance restored for compute phase")
                
                # Track performance
                if perf_metrics['ipc'] > 0:
                    self.performance_history.append(perf_metrics['ipc'])
                    if self.baseline_performance is None and len(self.performance_history) >= 10:
                        self.baseline_performance = np.mean(self.performance_history)
                
                # Log metrics
                self.log_metrics(
                    timestamp=current_time - start_time,
                    phase=phase,
                    metrics=perf_metrics,
                    mpi_activity=mpi_activity,
                    power=power,
                    actions=actions
                )
                
                # Console output (every 5 iterations)
                iteration += 1
                if iteration % 5 == 0:
                    energy_saved = (1.0 - power / self.power_monitor.baseline_power) * 100 if self.power_monitor.baseline_power else 0
                    perf_impact = self.calculate_performance_impact()
                    
                    print(f"[{current_time - start_time:6.1f}s] "
                          f"Phase={phase:12s} | "
                          f"Power={power:5.1f}W | "
                          f"IPC={perf_metrics.get('ipc', 0):0.2f} | "
                          f"CommWait={mpi_activity.get('wait_ratio', 0):0.2f} | "
                          f"Freq={self.freq_controller.current_freq}MHz | "
                          f"EnergySaved={energy_saved:+.1f}% | "
                          f"PerfImpact={perf_impact:+.1f}%")
                
                # Maintain timing
                last_time = current_time
                elapsed = time.time() - loop_start
                sleep_time = max(0, CONFIG.PERF_INTERVAL - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            print("\n[INTERRUPT] Monitor stopped by user")
        
        finally:
            self.cleanup
