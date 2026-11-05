#!/usr/bin/env python3
import glob
import json
import matplotlib.pyplot as plt
import numpy as np
import re
import sys
from datetime import datetime

def parse_size_from_filename(filename):
    """Extract problem size from filename"""
    match = re.search(r'memory_size_(\d+)', filename)
    return int(match.group(1)) if match else 0

def analyze_comprehensive_memory(baseline_log, optimized_log, size_logs):
    """Comprehensive analysis of memory-bound phase across problem sizes"""
    
    print("Performing comprehensive memory-bound analysis...")
    
    # Parse all log files
    results = {}
    
    # Parse baseline and optimized
    results['baseline'] = parse_memory_log(baseline_log)
    results['optimized'] = parse_memory_log(optimized_log)
    
    # Parse size-specific logs
    for log_file in size_logs:
        size = parse_size_from_filename(log_file)
        if size > 0:
            results[f'size_{size}'] = parse_memory_log(log_file)
    
    # Generate comprehensive analysis
    generate_memory_comprehensive_report(results)
    
    return results

def parse_memory_log(log_file):
    """Parse memory-specific metrics from log file"""
    data = {}
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        # Extract key metrics
        metrics = {
            'energy_pkg': r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/',
            'energy_ram': r'(\d+\.?\d*)\s+Joules\s+power/energy-ram/',
            'cache_misses': r'(\d+[,]?\d*)\s+cache-misses',
            'cache_references': r'(\d+[,]?\d*)\s+cache-references',
            'instructions': r'(\d+[,]?\d*)\s+instructions',
            'cycles': r'(\d+[,]?\d*)\s+cpu-cycles',
            'runtime': r'(\d+\.\d+)\s+seconds time elapsed'
        }
        
        for key, pattern in metrics.items():
            match = re.search(pattern, content)
            if match:
                value = match.group(1).replace(',', '')
                data[key] = float(value)
        
        # Calculate derived metrics
        if 'cache_references' in data and data['cache_references'] > 0:
            data['cache_miss_rate'] = (data.get('cache_misses', 0) / data['cache_references']) * 100
            
        if 'instructions' in data and 'cycles' in data and data['cycles'] > 0:
            data['ipc'] = data['instructions'] / data['cycles']
            
        if 'energy_pkg' in data and 'runtime' in data and data['runtime'] > 0:
            data['power'] = data['energy_pkg'] / data['runtime']
            
    except Exception as e:
        print(f"Error parsing {log_file}: {e}")
    
    return data

def generate_memory_comprehensive_report(results):
    """Generate comprehensive memory analysis report"""
    
    print("\n" + "="*70)
    print("COMPREHENSIVE MEMORY-BOUND PHASE ANALYSIS")
    print("="*70)
    
    # Baseline vs Optimized comparison
    if 'baseline' in results and 'optimized' in results:
        baseline = results['baseline']
        optimized = results['optimized']
        
        energy_improvement = ((baseline.get('energy_pkg', 0) - optimized.get('energy_pkg', 0)) / 
                             baseline.get('energy_pkg', 1) * 100) if baseline.get('energy_pkg', 0) > 0 else 0
        performance_impact = ((optimized.get('runtime', 0) - baseline.get('runtime', 0)) / 
                             baseline.get('runtime', 1) * 100) if baseline.get('runtime', 0) > 0 else 0
        
        print(f"\nPrimary Optimization Results:")
        print(f"Energy Reduction: {energy_improvement:.2f}%")
        print(f"Performance Impact: {performance_impact:.2f}%")
        print(f"Cache Miss Rate Improvement: {baseline.get('cache_miss_rate', 0) - optimized.get('cache_miss_rate', 0):.2f}%")
    
    # Size scaling analysis
    size_data = {}
    for key in results:
        if key.startswith('size_'):
            size = int(key.split('_')[1])
            size_data[size] = results[key]
    
    if size_data:
        print(f"\nProblem Size Scaling Analysis:")
        sizes = sorted(size_data.keys())
        for size in sizes:
            data = size_data[size]
            print(f"Size {size}: Energy={data.get('energy_pkg', 0):.2f}J, "
                  f"Time={data.get('runtime', 0):.3f}s, "
                  f"MissRate={data.get('cache_miss_rate', 0):.2f}%")
    
    # Generate comprehensive visualization
    generate_memory_scaling_plot(results)
    
    # Save comprehensive report
    report = {
        'timestamp': datetime.now().isoformat(),
        'analysis_type': 'Memory-bound Phase Comprehensive',
        'primary_comparison': {
            'energy_improvement': energy_improvement,
            'performance_impact': performance_impact,
            'optimization_effective': energy_improvement >= 12 and abs(performance_impact) <= 5
        },
        'size_scaling': size_data,
        'recommendations': generate_memory_recommendations(results)
    }
    
    with open('memory_comprehensive_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nComprehensive analysis completed. Report saved.")

def generate_memory_scaling_plot(results):
    """Generate scaling analysis visualization"""
    try:
        # Extract size-based results
        sizes = []
        energies = []
        times = []
        miss_rates = []
        
        for key in results:
            if key.startswith('size_'):
                size = int(key.split('_')[1])
                data = results[key]
                sizes.append(size)
                energies.append(data.get('energy_pkg', 0))
                times.append(data.get('runtime', 0))
                miss_rates.append(data.get('cache_miss_rate', 0))
        
        if not sizes:
            return
        
        # Create subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Energy vs Problem Size
        ax1.plot(sizes, energies, 'bo-', linewidth=2, markersize=8)
        ax1.set_xscale('log')
        ax1.set_yscale('log')
        ax1.set_xlabel('Problem Size')
        ax1.set_ylabel('Energy Consumption (J)')
        ax1.set_title('Energy vs Problem Size')
        ax1.grid(True, alpha=0.3)
        
        # Time vs Problem Size
        ax2.plot(sizes, times, 'ro-', linewidth=2, markersize=8)
        ax2.set_xscale('log')
        ax2.set_yscale('log')
        ax2.set_xlabel('Problem Size')
        ax2.set_ylabel('Execution Time (s)')
        ax2.set_title('Time vs Problem Size')
        ax2.grid(True, alpha=0.3)
        
        # Cache Miss Rate vs Problem Size
        ax3.plot(sizes, miss_rates, 'go-', linewidth=2, markersize=8)
        ax3.set_xscale('log')
        ax3.set_xlabel('Problem Size')
        ax3.set_ylabel('Cache Miss Rate (%)')
        ax3.set_title('Cache Behavior vs Problem Size')
        ax3.grid(True, alpha=0.3)
        
        # Energy-Time Efficiency
        efficiencies = [e/t if t > 0 else 0 for e, t in zip(energies, times)]
        ax4.plot(sizes, efficiencies, 'mo-', linewidth=2, markersize=8)
        ax4.set_xscale('log')
        ax4.set_xlabel('Problem Size')
        ax4.set_ylabel('Energy-Time Product (J·s)')
        ax4.set_title('Energy-Time Efficiency vs Problem Size')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('memory_scaling_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Scaling analysis plot saved as 'memory_scaling_analysis.png'")
        
    except Exception as e:
        print(f"Error generating scaling plot: {e}")

def generate_memory_recommendations(results):
    """Generate memory-specific optimization recommendations"""
    recommendations = []
    
    baseline = results.get('baseline', {})
    optimized = results.get('optimized', {})
    
    energy_improvement = ((baseline.get('energy_pkg', 0) - optimized.get('energy_pkg', 0)) / 
                         baseline.get('energy_pkg', 1) * 100) if baseline.get('energy_pkg', 0) > 0 else 0
    
    if energy_improvement >= 12:
        recommendations.append("Current memory-aware power optimization is effective")
    else:
        recommendations.append("Consider more aggressive memory power management")
    
    if optimized.get('cache_miss_rate', 0) < baseline.get('cache_miss_rate', 0):
        recommendations.append("Cache behavior improved with optimization")
    else:
        recommendations.append("Investigate cache performance degradation")
    
    # Size-based recommendations
    size_data = {}
    for key in results:
        if key.startswith('size_'):
            size = int(key.split('_')[1])
            size_data[size] = results[key]
    
    if size_data:
        largest_size = max(size_data.keys())
        largest_data = size_data[largest_size]
        if largest_data.get('cache_miss_rate', 0) > 20:
            recommendations.append(f"High cache miss rate ({largest_data.get('cache_miss_rate', 0):.1f}%) at large problem sizes - consider algorithm optimization")
    
    return "; ".join(recommendations)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python memory_comprehensive_analysis.py <baseline_log> <optimized_log> [size_logs...]")
        sys.exit(1)
    
    baseline_log = sys.argv[1]
    optimized_log = sys.argv[2]
    size_logs = sys.argv[3:] if len(sys.argv) > 3 else []
    
    analyze_comprehensive_memory(baseline_log, optimized_log, size_logs)


