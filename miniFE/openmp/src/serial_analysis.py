#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import json
import re

def parse_serial_logs(*log_files):
    """Parse multiple serial phase log files"""
    data = {}
    for log_file in log_files:
        data[log_file] = parse_serial_log_simple(log_file)
    return data

def parse_serial_log_simple(log_file):
    """Simple parser for serial log files"""
    data = {}
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        energy_match = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
        time_match = re.search(r'(\d+\.\d+)\s+seconds time elapsed', content)
        instructions_match = re.search(r'(\d+[,]?\d*)\s+instructions', content)
        cycles_match = re.search(r'(\d+[,]?\d*)\s+cpu-cycles', content)
        
        if energy_match:
            data['energy'] = float(energy_match.group(1))
        if time_match:
            data['time'] = float(time_match.group(1))
        if instructions_match and cycles_match:
            instructions = float(instructions_match.group(1).replace(',', ''))
            cycles = float(cycles_match.group(1).replace(',', ''))
            data['ipc'] = instructions / cycles if cycles > 0 else 0
            
    except Exception as e:
        print(f"Error parsing {log_file}: {e}")
    
    return data

def analyze_serial_results(baseline_log, optimized_log, power_gated_log):
    """Analyze serial phase optimization results across different configurations"""
    
    print("Analyzing serial phase results...")
    data = parse_serial_logs(baseline_log, optimized_log, power_gated_log)
    
    configurations = ['Baseline', 'Optimized', 'Power Gated']
    log_files = [baseline_log, optimized_log, power_gated_log]
    
    results = {}
    for config, log_file in zip(configurations, log_files):
        results[config] = data.get(log_file, {})
    
    print("\n=== Serial Phase Comprehensive Analysis ===")
    for config in configurations:
        config_data = results[config]
        print(f"\n{config}:")
        print(f"  Energy: {config_data.get('energy', 0):.2f}J")
        print(f"  Time: {config_data.get('time', 0):.3f}s")
        print(f"  IPC: {config_data.get('ipc', 0):.4f}")
        if config_data.get('time', 0) > 0:
            print(f"  Power: {config_data.get('energy', 0)/config_data.get('time', 1):.2f}W")
    
    # Calculate improvements
    if 'Baseline' in results and 'Optimized' in results:
        baseline = results['Baseline']
        optimized = results['Optimized']
        
        energy_improvement = ((baseline.get('energy', 0) - optimized.get('energy', 0)) / 
                             baseline.get('energy', 1) * 100) if baseline.get('energy', 0) > 0 else 0
        time_impact = ((optimized.get('time', 0) - baseline.get('time', 0)) / 
                      baseline.get('time', 1) * 100) if baseline.get('time', 0) > 0 else 0
        
        print(f"\nOptimization Improvements:")
        print(f"Energy Reduction: {energy_improvement:.2f}%")
        print(f"Time Impact: {time_impact:.2f}%")
    
    # Generate comprehensive visualization
    generate_serial_comprehensive_plot(results, configurations)
    
    return results

def generate_serial_comprehensive_plot(results, configurations):
    """Generate comprehensive visualization of serial phase results"""
    try:
        metrics = ['Energy (J)', 'Time (s)', 'IPC', 'Power (W)']
        data_to_plot = {}
        
        for config in configurations:
            config_data = results[config]
            data_to_plot[config] = [
                config_data.get('energy', 0),
                config_data.get('time', 0),
                config_data.get('ipc', 0),
                config_data.get('energy', 0) / config_data.get('time', 1) if config_data.get('time', 0) > 0 else 0
            ]
        
        x = np.arange(len(metrics))
        width = 0.25
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        for i, config in enumerate(configurations):
            offset = width * (i - 1)
            bars = ax.bar(x + offset, data_to_plot[config], width, label=config, alpha=0.7)
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.2f}',
                       ha='center', va='bottom', fontsize=8)
        
        ax.set_ylabel('Values')
        ax.set_title('Serial Phase Optimization - Comprehensive Analysis')
        ax.set_xticks(x)
        ax.set_xticklabels(metrics)
        ax.legend()
        
        plt.tight_layout()
        plt.savefig('serial_comprehensive_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Comprehensive analysis plot saved as 'serial_comprehensive_analysis.png'")
        
    except Exception as e:
        print(f"Error generating comprehensive plot: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python serial_analysis.py <baseline_log> <optimized_log> <power_gated_log>")
        sys.exit(1)
    
    analyze_serial_results(sys.argv[1], sys.argv[2], sys.argv[3])
