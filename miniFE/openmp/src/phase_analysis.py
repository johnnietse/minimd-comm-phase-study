#!/usr/bin/env python3
import re
import json
import sys

def analyze_phases(log_file):
    """Analyze computational phases from performance log"""
    phases = {
        'initialization': {'energy': 0, 'time': 0},
        'computation': {'energy': 0, 'time': 0},
        'finalization': {'energy': 0, 'time': 0}
    }
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        # Extract overall metrics
        total_energy = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
        total_time = re.search(r'(\d+\.\d+)\s+seconds time elapsed', content)
        
        if total_energy and total_time:
            energy = float(total_energy.group(1))
            time = float(total_time.group(1))
            
            # Estimate phase distribution (this would ideally come from instrumented code)
            # For now, using typical HPC application distribution
            phases['initialization']['energy'] = energy * 0.1  # 10% for initialization
            phases['initialization']['time'] = time * 0.1
            
            phases['computation']['energy'] = energy * 0.8  # 80% for computation
            phases['computation']['time'] = time * 0.8
            
            phases['finalization']['energy'] = energy * 0.1  # 10% for finalization
            phases['finalization']['time'] = time * 0.1
            
        print("Phase Analysis Results:")
        print(f"Total Time: {time:.3f}s")
        print(f"Total Energy: {energy:.2f}J")
        print("\nPhase Distribution (Estimated):")
        for phase, metrics in phases.items():
            print(f"{phase.capitalize()}:")
            print(f"  Time: {metrics['time']:.3f}s ({metrics['time']/time*100:.1f}%)")
            print(f"  Energy: {metrics['energy']:.2f}J ({metrics['energy']/energy*100:.1f}%)")
            
        # Save phase analysis
        with open('phase_analysis.json', 'w') as f:
            json.dump(phases, f, indent=2)
            
        return phases
        
    except Exception as e:
        print(f"Error analyzing phases: {e}")
        return {}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python phase_analysis.py <log_file>")
        sys.exit(1)
    
    analyze_phases(sys.argv[1])
