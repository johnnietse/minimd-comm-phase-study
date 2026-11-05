#!/usr/bin/env python3
"""
Complete miniMD Phase Monitor - All Phases
"""

import subprocess
import time
import csv
import os
import numpy as np
from collections import deque

class MiniMDPhaseMonitor:
    def __init__(self):
        self.phases = {
            'INITIALIZATION': {'idle': [70, 90], 'power': [60, 90], 'ctx': [100, 1000]},
            'COMPUTE_FORCE': {'idle': [10, 50], 'power': [100, 200], 'ctx': [10, 100]},
            'NEIGHBOR_REBUILD': {'idle': [50, 80], 'power': [80, 120], 'ctx': [500, 5000]},
            'COMM_EXCHANGE': {'idle': [90, 99], 'power': [70, 85], 'ctx': [1000, 50000]},
            'COMM_BORDERS': {'idle': [92, 99], 'power': [70, 82], 'ctx': [5000, 20000]},
            'COMM_REVERSE': {'idle': [91, 99], 'power': [71, 83], 'ctx': [2000, 30000]},
            'IO_OUTPUT': {'idle': [60, 85], 'power': [65, 80], 'iowait': [5, 50]},
            'BARRIER_SYNC': {'idle': [95, 99], 'power': [65, 75], 'ctx': [100, 1000]},
            'FINALIZATION': {'idle': [80, 95], 'power': [60, 75], 'ctx': [100, 500]}
        }
        
        # Sliding window for pattern recognition
        self.metric_window = deque(maxlen=5)
        self.phase_history = []
        
    def detect_all_phases(self, current_metrics):
        """
        Detect ALL phases, not just communication
        """
        # Add current metrics to window
        self.metric_window.append(current_metrics)
        
        # Calculate moving averages
        if len(self.metric_window) >= 3:
            avg_idle = np.mean([m['idle'] for m in list(self.metric_window)[-3:]])
            avg_power = np.mean([m['power'] for m in list(self.metric_window)[-3:]])
            avg_ctx = np.mean([m['ctx_rate'] for m in list(self.metric_window)[-3:]])
            avg_iowait = current_metrics.get('iowait', 0)
        else:
            avg_idle = current_metrics['idle']
            avg_power = current_metrics['power']
            avg_ctx = current_metrics['ctx_rate']
            avg_iowait = current_metrics.get('iowait', 0)
        
        # Phase scoring
        phase_scores = {}
        
        for phase_name, phase_ranges in self.phases.items():
            score = 0.0
            
            # Check idle range
            if phase_ranges['idle'][0] <= avg_idle <= phase_ranges['idle'][1]:
                score += 0.4
            elif avg_idle > phase_ranges['idle'][1]:
                # Too high idle for this phase
                penalty = (avg_idle - phase_ranges['idle'][1]) / 10
                score -= min(0.2, penalty)
            
            # Check power range
            if phase_ranges['power'][0] <= avg_power <= phase_ranges['power'][1]:
                score += 0.3
            elif avg_power > phase_ranges['power'][1]:
                # Too high power
                penalty = (avg_power - phase_ranges['power'][1]) / 50
                score -= min(0.15, penalty)
            
            # Check context switch range
            if phase_ranges['ctx'][0] <= avg_ctx <= phase_ranges['ctx'][1]:
                score += 0.3
            elif avg_ctx > phase_ranges['ctx'][1]:
                # Too many context switches
                penalty = (avg_ctx - phase_ranges['ctx'][1]) / 10000
                score -= min(0.15, penalty)
            
            # Check iowait for IO phase
            if 'iowait' in phase_ranges:
                if phase_ranges['iowait'][0] <= avg_iowait <= phase_ranges['iowait'][1]:
                    score += 0.4
            
            phase_scores[phase_name] = score
        
        # Find best matching phase
        best_phase = max(phase_scores, key=phase_scores.get)
        best_score = phase_scores[best_phase]
        
        # Threshold for confidence
        if best_score < 0.6:
            # Use pattern-based detection if scores are low
            best_phase = self.detect_by_pattern(current_metrics)
            best_score = 0.5
        
        return best_phase, best_score
    
    def detect_by_pattern(self, metrics):
        """
        Use pattern recognition based on phase sequence
        """
        # Expected pattern in miniMD:
        # INIT → [COMPUTE → COMM]*100, with NEIGHBOR every 20 steps
        
        if not self.phase_history:
            return "INITIALIZATION"
        
        last_phase = self.phase_history[-1][0] if self.phase_history else "INITIALIZATION"
        
        # Pattern-based transitions
        if last_phase == "INITIALIZATION":
            return "COMPUTE_FORCE"
        elif last_phase == "COMPUTE_FORCE":
            # Check if this is a neighbor rebuild step (every 20 compute steps)
            compute_count = sum(1 for p in self.phase_history if p[0] == "COMPUTE_FORCE")
            if compute_count % 20 == 0:
                return "NEIGHBOR_REBUILD"
            else:
                return "COMM_EXCHANGE"
        elif last_phase == "NEIGHBOR_REBUILD":
            return "COMM_EXCHANGE"
        elif last_phase.startswith("COMM_"):
            return "COMPUTE_FORCE"
        else:
            return "UNKNOWN"
    
    def log_phase_transition(self, old_phase, new_phase, timestamp, duration):
        """
        Log phase transitions with duration
        """
        if old_phase != new_phase:
            print(f"[{timestamp:.3f}s] {old_phase:20} → {new_phase:20} (duration: {duration:.3f}s)")
            self.phase_history.append((new_phase, timestamp, duration))

# Main monitoring loop with multi-phase detection
def monitor_all_phases():
    monitor = MiniMDPhaseMonitor()
    current_phase = None
    phase_start_time = time.time()
    
    while True:
        # Sample metrics
        metrics = {
            'idle': get_cpu_idle(),
            'power': get_current_power(),
            'ctx_rate': get_context_switch_rate(),
            'iowait': get_iowait()
        }
        
        # Detect phase
        detected_phase, confidence = monitor.detect_all_phases(metrics)
        timestamp = time.time() - global_start
        
        # Track phase duration
        if detected_phase != current_phase:
            phase_duration = timestamp - phase_start_time
            if current_phase:
                monitor.log_phase_transition(current_phase, detected_phase, 
                                           timestamp, phase_duration)
            current_phase = detected_phase
            phase_start_time = timestamp
        
        # Log detailed metrics
        log_metrics(timestamp, detected_phase, confidence, metrics)
