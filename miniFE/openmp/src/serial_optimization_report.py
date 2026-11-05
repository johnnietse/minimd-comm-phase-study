#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import numpy as np
import sys
from datetime import datetime

def generate_serial_report(baseline_log, optimized_log):
    """Generate comprehensive serial phase optimization report"""
    
    print("Generating serial phase optimization report...")
    
    # Load analysis results
    try:
        with open('serial_phase_validation.json', 'r') as f:
            validation_results = json.load(f)
    except:
        validation_results = {}
    
    try:
        with open('serial_phases.json', 'r') as f:
            phase_analysis = json.load(f)
    except:
        phase_analysis = {}
    
    # Generate report
    report = {
        'timestamp': datetime.now().isoformat(),
        'test_configuration': {
            'application': 'MiniFE',
            'phase': 'Serial Computation',
            'threads': 1,
            'optimization': 'Power management during serial phases'
        },
        'results': validation_results,
        'phase_characteristics': phase_analysis,
        'summary': {
            'status': 'PASS' if validation_results.get('overall_pass', False) else 'FAIL',
            'energy_savings': validation_results.get('energy_reduction', 0),
            'performance_impact': validation_results.get('time_impact', 0),
            'recommendations': generate_recommendations(validation_results)
        }
    }
    
    # Print report summary
    print("\n" + "="*60)
    print("SERIAL PHASE OPTIMIZATION REPORT")
    print("="*60)
    print(f"Status: {report['summary']['status']}")
    print(f"Energy Savings: {report['summary']['energy_savings']:.2f}%")
    print(f"Performance Impact: {report['summary']['performance_impact']:.3f}%")
    print(f"Recommendations: {report['summary']['recommendations']}")
    print("="*60)
    
    # Save detailed report
    with open('serial_optimization_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    # Generate visual report
    generate_visual_report(report)
    
    return report

def generate_recommendations(results):
    """Generate optimization recommendations based on results"""
    recommendations = []
    
    if results.get('energy_target_met', False):
        recommendations.append("Continue using current power optimization settings")
    else:
        recommendations.append("Consider more aggressive power reduction during serial phases")
    
    if results.get('time_target_met', False):
        recommendations.append("Performance impact is within acceptable limits")
    else:
        recommendations.append("Reduce power optimization aggressiveness to maintain performance")
    
    if results.get('ipc_target_met', False):
        recommendations.append("Single-thread performance is preserved")
    else:
        recommendations.append("Investigate IPC degradation causes")
    
    return "; ".join(recommendations)

def generate_visual_report(report):
    """Generate visual summary report"""
    try:
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Target status
        targets = ['Time Impact', 'Energy Reduction', 'IPC Maintenance']
        target_status = [
            report['results'].get('time_target_met', False),
            report['results'].get('energy_target_met', False),
            report['results'].get('ipc_target_met', False)
        ]
        colors = ['green' if status else 'red' for status in target_status]
        
        ax1.bar(targets, [1, 1, 1], color=colors, alpha=0.7)
        ax1.set_title('Target Achievement Status')
        ax1.set_ylabel('Status (1 = Met)')
        
        # Add status labels
        for i, (target, status) in enumerate(zip(targets, target_status)):
            ax1.text(i, 0.5, 'MET' if status else 'NOT MET', 
                    ha='center', va='center', fontsize=12, 
                    color='white', fontweight='bold')
        
        # Energy and performance impact
        metrics = ['Energy Reduction (%)', 'Performance Impact (%)']
        values = [
            report['results'].get('energy_reduction', 0),
            report['results'].get('time_impact', 0)
        ]
        colors = ['green' if values[0] >= 20 else 'orange', 
                 'green' if abs(values[1]) < 1 else 'orange']
        
        bars = ax2.bar(metrics, values, color=colors, alpha=0.7)
        ax2.set_title('Optimization Effectiveness')
        ax2.set_ylabel('Percentage (%)')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}%',
                    ha='center', va='bottom')
        
        # Overall status
        status = report['summary']['status']
        color = 'green' if status == 'PASS' else 'red'
        ax3.bar(['Overall'], [1], color=color, alpha=0.7)
        ax3.set_title('Overall Test Result')
        ax3.set_ylim(0, 1.2)
        ax3.text(0, 0.5, status, ha='center', va='center', 
                fontsize=20, color='white', fontweight='bold')
        
        # Recommendations
        recommendations = report['summary']['recommendations'].split('; ')
        ax4.axis('off')
        ax4.set_title('Recommendations')
        for i, rec in enumerate(recommendations):
            ax4.text(0.1, 0.9 - i*0.15, f'• {rec}', 
                    fontsize=10, transform=ax4.transAxes,
                    verticalalignment='top')
        
        plt.tight_layout()
        plt.savefig('serial_optimization_summary.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Visual report saved as 'serial_optimization_summary.png'")
        
    except Exception as e:
        print(f"Error generating visual report: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python serial_optimization_report.py <baseline_log> <optimized_log>")
        sys.exit(1)
    
    generate_serial_report(sys.argv[1], sys.argv[2])
