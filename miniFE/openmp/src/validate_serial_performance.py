#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import re
import json

def parse_serial_log(log_file):
    """Parse serial phase specific metrics from log file"""
    data = {}
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
            # Extract energy measurements
            energy_pkg_match = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
            energy_ram_match = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-ram/', content)
            
            # Extract performance counters
            instructions_match = re.search(r'(\d+[,]?\d*)\s+instructions', content)
            cycles_match = re.search(r'(\d+[,]?\d*)\s+cpu-cycles', content)
            
            # Extract runtime
            runtime_match = re.search(r'(\d+\.\d+)\s+seconds time elapsed', content)
            
            if energy_pkg_match:
                data['energy_pkg'] = float(energy_pkg_match.group(1).replace(',', ''))
            if energy_ram_match:
                data['energy_ram'] = float(energy_ram_match.group(1).replace(',', ''))
            if instructions_match:
                data['instructions'] = float(instructions_match.group(1).replace(',', ''))
            if cycles_match:
                data['cycles'] = float(cycles_match.group(1).replace(',', ''))
            if runtime_match:
                data['runtime'] = float(runtime_match.group(1))
                
            # Calculate derived metrics
            if 'instructions' in data and 'cycles' in data and data['cycles'] > 0:
                data['ipc'] = data['instructions'] / data['cycles']
                
    except Exception as e:
        print(f"Error parsing log file {log_file}: {e}")
    
    return data

def validate_serial_performance(baseline_log, optimized_log):
    """Validate serial phase optimization meets performance constraints"""
    
    print("Parsing serial phase log files...")
    baseline_data = parse_serial_log(baseline_log)
    optimized_data = parse_serial_log(optimized_log)
    
    if not baseline_data or not optimized_data:
        print("Error: Could not parse one or both log files")
        return
    
    print("\n=== Serial Phase Performance Validation ===")
    
    # Calculate performance metrics
    time_impact = ((optimized_data.get('runtime', 0) - baseline_data.get('runtime', 0)) / 
                  baseline_data.get('runtime', 1) * 100) if baseline_data.get('runtime', 0) > 0 else 0
    
    energy_reduction = ((baseline_data.get('energy_pkg', 0) - optimized_data.get('energy_pkg', 0)) / 
                       baseline_data.get('energy_pkg', 1) * 100) if baseline_data.get('energy_pkg', 0) > 0 else 0
    
    ipc_ratio = optimized_data.get('ipc', 0) / baseline_data.get('ipc', 1) if baseline_data.get('ipc', 0) > 0 else 0
    
    print(f"Time Impact: {time_impact:.3f}% (Target: <1%)")
    print(f"Energy Reduction: {energy_reduction:.2f}% (Target: 20-25%)")
    print(f"IPC Ratio: {ipc_ratio:.3f} (Target: >0.99)")
    print(f"Baseline Runtime: {baseline_data.get('runtime', 0):.3f}s")
    print(f"Optimized Runtime: {optimized_data.get('runtime', 0):.3f}s")
    print(f"Baseline Energy: {baseline_data.get('energy_pkg', 0):.2f}J")
    print(f"Optimized Energy: {optimized_data.get('energy_pkg', 0):.2f}J")
    
    # Check if targets are met
    time_target_met = abs(time_impact) < 1.0
    energy_target_met = energy_reduction >= 20
    ipc_target_met = ipc_ratio >= 0.99
    
    targets_met = time_target_met and energy_target_met and ipc_target_met
    
    print(f"\n=== Target Validation ===")
    print(f"Time Target (<1%): {'MET' if time_target_met else 'NOT MET'}")
    print(f"Energy Target (≥20%): {'MET' if energy_target_met else 'NOT MET'}")
    print(f"IPC Target (≥0.99): {'MET' if ipc_target_met else 'NOT MET'}")
    print(f"Overall: {'PASS' if targets_met else 'FAIL'}")
    
    # Generate validation report
    generate_serial_report(baseline_data, optimized_data, targets_met)
    
    # Save results to JSON
    results = {
        'time_impact': time_impact,
        'energy_reduction': energy_reduction,
        'ipc_ratio': ipc_ratio,
        'time_target_met': time_target_met,
        'energy_target_met': energy_target_met,
        'ipc_target_met': ipc_target_met,
        'overall_pass': targets_met
    }
    
    with open('serial_phase_validation.json', 'w') as f:
        json.dump(results, f, indent=2)

def generate_serial_report(baseline, optimized, targets_met):
    """Generate comprehensive serial phase validation report"""
    try:
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
        
        # Time comparison
        times = [baseline.get('runtime', 0), optimized.get('runtime', 0)]
        bars1 = ax1.bar(['Baseline', 'Optimized'], times, color=['blue', 'orange'], alpha=0.7)
        ax1.set_ylabel('Execution Time (s)')
        ax1.set_title('Serial Phase Execution Time')
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.3f}s',
                    ha='center', va='bottom')
        
        # Energy comparison
        energies = [baseline.get('energy_pkg', 0), optimized.get('energy_pkg', 0)]
        bars2 = ax2.bar(['Baseline', 'Optimized'], energies, color=['blue', 'orange'], alpha=0.7)
        ax2.set_ylabel('Energy Consumption (J)')
        ax2.set_title('Serial Phase Energy Consumption')
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}J',
                    ha='center', va='bottom')
        
        # IPC comparison
        ipcs = [baseline.get('ipc', 0), optimized.get('ipc', 0)]
        bars3 = ax3.bar(['Baseline', 'Optimized'], ipcs, color=['blue', 'orange'], alpha=0.7)
        ax3.set_ylabel('Instructions per Cycle')
        ax3.set_title('Single-Thread Performance (IPC)')
        for bar in bars3:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.4f}',
                    ha='center', va='bottom')
        
        # Target validation
        status = 'PASS' if targets_met else 'FAIL'
        colors = 'green' if targets_met else 'red'
        ax4.bar(['Validation'], [1], color=colors, alpha=0.7)
        ax4.set_ylabel('Status')
        ax4.set_title('Performance Target Validation')
        ax4.text(0, 0.5, status, ha='center', va='center', fontsize=20, color='white', fontweight='bold')
        ax4.set_ylim(0, 1.2)
        
        plt.tight_layout()
        plt.savefig('serial_phase_validation.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Validation report saved as 'serial_phase_validation.png'")
        
    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python validate_serial_performance.py <baseline_log> <optimized_log>")
        sys.exit(1)
    
    validate_serial_performance(sys.argv[1], sys.argv[2])
