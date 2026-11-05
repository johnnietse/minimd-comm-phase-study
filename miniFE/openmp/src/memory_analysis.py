#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import re
import json
from pathlib import Path

def parse_perf_log(log_file):
    """Parse perf stat output log file"""
    data = {}
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
            # Extract energy measurements
            energy_pkg_match = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
            energy_ram_match = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-ram/', content)
            
            # Extract cache metrics
            cache_misses_match = re.search(r'(\d+[,]?\d*)\s+cache-misses', content)
            cache_refs_match = re.search(r'(\d+[,]?\d*)\s+cache-references', content)
            
            # Extract performance counters
            instructions_match = re.search(r'(\d+[,]?\d*)\s+instructions', content)
            cycles_match = re.search(r'(\d+[,]?\d*)\s+cpu-cycles', content)
            
            # Extract runtime
            runtime_match = re.search(r'(\d+\.\d+)\s+seconds time elapsed', content)
            if not runtime_match:
                # Try your custom format
                runtime_match = re.search(r'Runtime:\s*(\d+\.\d+)\s*seconds', content) 
            
            if energy_pkg_match:
                data['energy_pkg'] = float(energy_pkg_match.group(1).replace(',', ''))
            if energy_ram_match:
                data['energy_ram'] = float(energy_ram_match.group(1).replace(',', ''))
            if cache_misses_match:
                data['cache_misses'] = float(cache_misses_match.group(1).replace(',', ''))
            if cache_refs_match:
                data['cache_refs'] = float(cache_refs_match.group(1).replace(',', ''))
            if instructions_match:
                data['instructions'] = float(instructions_match.group(1).replace(',', ''))
            if cycles_match:
                data['cycles'] = float(cycles_match.group(1).replace(',', ''))
            if runtime_match:
                data['runtime'] = float(runtime_match.group(1))
                
            # Calculate derived metrics
            if 'cache_refs' in data and data['cache_refs'] > 0 and 'cache_misses' in data:
                data['cache_miss_rate'] = (data['cache_misses'] / data['cache_refs']) * 100
            if 'instructions' in data and 'cycles' in data and data['cycles'] > 0:
                data['ipc'] = data['instructions'] / data['cycles']
                
    except Exception as e:
        print(f"Error parsing log file {log_file}: {e}")
    
    return data

def analyze_memory_phase(baseline_log, optimized_log):
    """Analyze memory-bound phase optimization results"""
    
    print("Parsing log files...")
    baseline_data = parse_perf_log(baseline_log)
    optimized_data = parse_perf_log(optimized_log)
    
    if not baseline_data or not optimized_data:
        print("Error: Could not parse one or both log files")
        return
    
    print("\n=== Memory-Bound Phase Analysis Results ===")
    
    # Calculate improvements
    energy_reduction_pkg = ((baseline_data.get('energy_pkg', 0) - optimized_data.get('energy_pkg', 0)) / 
                           baseline_data.get('energy_pkg', 1) * 100) if baseline_data.get('energy_pkg', 0) > 0 else 0
    energy_reduction_ram = ((baseline_data.get('energy_ram', 0) - optimized_data.get('energy_ram', 0)) / 
                           baseline_data.get('energy_ram', 1) * 100) if baseline_data.get('energy_ram', 0) > 0 else 0
    
    cache_miss_improvement = ((baseline_data.get('cache_miss_rate', 0) - optimized_data.get('cache_miss_rate', 0)) / 
                             baseline_data.get('cache_miss_rate', 1) * 100) if baseline_data.get('cache_miss_rate', 0) > 0 else 0
    
    performance_impact = ((optimized_data.get('runtime', 0) / baseline_data.get('runtime', 1)) - 1) * 100 if baseline_data.get('runtime', 0) > 0 else 0
    
    print(f"Package Energy Reduction: {energy_reduction_pkg:.2f}%")
    print(f"RAM Energy Reduction: {energy_reduction_ram:.2f}%")
    print(f"Cache Miss Rate Improvement: {cache_miss_improvement:.2f}%")
    print(f"Performance Impact: {performance_impact:.2f}%")
    print(f"IPC Baseline: {baseline_data.get('ipc', 0):.4f}")
    print(f"IPC Optimized: {optimized_data.get('ipc', 0):.4f}")
    
    # Check against targets
    print("\n=== Target Validation ===")
    energy_target_met = energy_reduction_pkg >= 12
    performance_target_met = abs(performance_impact) <= 5
    
    print(f"Energy Target (≥12%): {'MET' if energy_target_met else 'NOT MET'}")
    print(f"Performance Target (≤5% impact): {'MET' if performance_target_met else 'NOT MET'}")
    print(f"Overall: {'PASS' if energy_target_met and performance_target_met else 'FAIL'}")
    
    # Generate visualization
    plot_memory_results(baseline_data, optimized_data)
    
    # Save results to JSON
    results = {
        'energy_reduction_pkg': energy_reduction_pkg,
        'energy_reduction_ram': energy_reduction_ram,
        'cache_miss_improvement': cache_miss_improvement,
        'performance_impact': performance_impact,
        'energy_target_met': energy_target_met,
        'performance_target_met': performance_target_met
    }
    
    with open('memory_phase_results.json', 'w') as f:
        json.dump(results, f, indent=2)

def plot_memory_results(baseline, optimized):
    """Plot memory-bound phase optimization results"""
    try:
        metrics = ['Energy (J)', 'Cache Miss Rate (%)', 'Runtime (s)', 'IPC']
        baseline_vals = [
            baseline.get('energy_pkg', 0),
            baseline.get('cache_miss_rate', 0),
            baseline.get('runtime', 0),
            baseline.get('ipc', 0)
        ]
        optimized_vals = [
            optimized.get('energy_pkg', 0),
            optimized.get('cache_miss_rate', 0),
            optimized.get('runtime', 0),
            optimized.get('ipc', 0)
        ]
        
        x = np.arange(len(metrics))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(12, 8))
        bars1 = ax.bar(x - width/2, baseline_vals, width, label='Baseline', alpha=0.7, color='blue')
        bars2 = ax.bar(x + width/2, optimized_vals, width, label='Optimized', alpha=0.7, color='orange')
        
        ax.set_ylabel('Values')
        ax.set_title('Memory-Bound Phase Optimization Results')
        ax.set_xticks(x)
        ax.set_xticklabels(metrics)
        ax.legend()
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.2f}',
                       ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('memory_phase_optimization.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Visualization saved as 'memory_phase_optimization.png'")
        
    except Exception as e:
        print(f"Error generating plot: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python memory_analysis.py <baseline_log> <optimized_log>")
        sys.exit(1)
    
    analyze_memory_phase(sys.argv[1], sys.argv[2])
