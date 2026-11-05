#!/usr/bin/env python3
"""
Communication Phase Analysis Tool
Analyzes the output from communication_monitor.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def analyze_communication_log(csv_path="communication_phase_monitor.csv"):
    """Generate comprehensive communication phase analysis."""
    
    df = pd.read_csv(csv_path)
    
    # Communication phase isolation
    comm_phases = df[df['phase'] == 'COMMUNICATION']
    
    if len(comm_phases) == 0:
        print("No communication phases detected!")
        return
    
    # Statistics
    print("=== Communication Phase Analysis ===\n")
    print(f"Total samples: {len(df)}")
    print(f"Communication samples: {len(comm_phases)} ({len(comm_phases)/len(df)*100:.1f}%)")
    print(f"\nCommunication Phase Metrics:")
    print(f"  Avg IPC:            {comm_phases['ipc'].mean():.3f}")
    print(f"  Avg MPI wait:       {comm_phases['mpi_wait_pct'].mean():.1%}")
    print(f"  Avg sync overhead:  {comm_phases['sync_overhead'].mean():.1%}")
    print(f"  Avg power:          {comm_phases['energy_W'].mean():.2f}W")
    print(f"  Avg confidence:     {comm_phases['confidence'].mean():.2f}")
    
    # Energy comparison
    comm_energy = (comm_phases['energy_W'] * 1.0).sum()  # Joules (W * 1s sampling)
    total_energy = (df['energy_W'] * 1.0).sum()
    
    print(f"\nEnergy Analysis:")
    print(f"  Communication energy: {comm_energy:.2f}J ({comm_energy/total_energy*100:.1f}% of total)")
    print(f"  Total energy:         {total_energy:.2f}J")
    
    # Visualization
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Timeline
    axes[0,0].plot(df['timestamp'], df['mpi_wait_pct'], label='MPI Wait %')
    axes[0,0].plot(df['timestamp'], df['sync_overhead'], label='Sync Overhead')
    axes[0,0].axhline(y=0.30, color='r', linestyle='--', label='MPI Threshold')
    axes[0,0].set_xlabel('Time (s)')
    axes[0,0].set_ylabel('Ratio')
    axes[0,0].set_title('Communication Indicators Over Time')
    axes[0,0].legend()
    axes[0,0].grid(True, alpha=0.3)
    
    # Phase distribution
    phase_counts = df['phase'].value_counts()
    axes[0,1].pie(phase_counts, labels=phase_counts.index, autopct='%1.1f%%')
    axes[0,1].set_title('Phase Distribution')
    
    # IPC vs Power (colored by phase)
    for phase in df['phase'].unique():
        phase_data = df[df['phase'] == phase]
        axes[1,0].scatter(phase_data['ipc'], phase_data['energy_W'], 
                          label=phase, alpha=0.6, s=20)
    axes[1,0].set_xlabel('IPC')
    axes[1,0].set_ylabel('Power (W)')
    axes[1,0].set_title('IPC vs Power by Phase')
    axes[1,0].legend()
    axes[1,0].grid(True, alpha=0.3)
    
    # Communication metrics correlation
    axes[1,1].scatter(comm_phases['mpi_wait_pct'], comm_phases['sync_overhead'], 
                      c=comm_phases['energy_W'], cmap='viridis', s=30)
    axes[1,1].set_xlabel('MPI Wait %')
    axes[1,1].set_ylabel('Sync Overhead')
    axes[1,1].set_title('Communication Behavior (colored by power)')
    axes[1,1].grid(True, alpha=0.3)
    cbar = plt.colorbar(axes[1,1].collections[0], ax=axes[1,1])
    cbar.set_label('Power (W)')
    
    plt.tight_layout()
    plt.savefig('communication_analysis.png', dpi=300)
    print(f"\nVisualization saved to communication_analysis.png")
    plt.show()

if __name__ == "__main__":
    analyze_communication_log()
