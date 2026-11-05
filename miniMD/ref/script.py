#!/usr/bin/env python3
"""
Communication Phase Analysis Tool
Analyzes the output from monitor.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os

def analyze_communication_log(csv_path="communication_phase_monitor.csv"):
    """Generate comprehensive communication phase analysis."""
    
    if not os.path.exists(csv_path):
        print(f"Error: Could not find {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Check for required columns (handling unit/no-unit variations)
    # Using 'power(W)' or 'energy_W' or similar
    power_col = 'power(W)' if 'power(W)' in df.columns else 'energy_W' # fallback
    energy_col = 'energy(J)' if 'energy(J)' in df.columns else 'energy_J' # fallback
    mpi_wait_col = 'mpi_wait_pct(%)' if 'mpi_wait_pct(%)' in df.columns else 'mpi_wait_pct'
    timestamp_col = 'timestamp(s)' if 'timestamp(s)' in df.columns else 'timestamp'

    print(f"Columns found: {df.columns.tolist()}")

    # Communication phase isolation
    comm_phases = df[df['phase'] == 'COMMUNICATION']
    
    if len(comm_phases) == 0:
        print("No communication phases detected!")
        # Continue to show overall stats even if no comm phase
    
    # Statistics
    print("=== Communication Phase Analysis ===\n")
    print(f"Total samples: {len(df)}")
    print(f"Communication samples: {len(comm_phases)} ({len(comm_phases)/len(df)*100:.1f}%)")
    
    if len(comm_phases) > 0:
        print(f"\nCommunication Phase Metrics:")
        print(f"  Avg IPC:            {comm_phases['ipc'].mean():.3f}")
        if mpi_wait_col in df.columns:
            print(f"  Avg MPI wait:       {comm_phases[mpi_wait_col].mean():.1%}")
        if 'sync_overhead(%)' in df.columns:
             print(f"  Avg sync overhead:  {comm_phases['sync_overhead(%)'].mean():.1%}")
        elif 'sync_overhead' in df.columns:
             print(f"  Avg sync overhead:  {comm_phases['sync_overhead'].mean():.1%}")

        if power_col in df.columns:
            print(f"  Avg power:          {comm_phases[power_col].mean():.2f}W")
        
        print(f"  Avg confidence:     {comm_phases['confidence'].mean():.2f}")
    
    # Energy comparison
    if energy_col in df.columns:
         comm_energy = df.loc[df['phase'] == 'COMMUNICATION', energy_col].sum()
         total_energy = df[energy_col].sum()
    elif power_col in df.columns:
         # Fallback if energy column missing: W * interval
         # Estimate interval from timestamp diff
         interval = 0.1 # assuming default
         if len(df) > 1:
             interval = df[timestamp_col].diff().median()
         
         comm_energy = (comm_phases[power_col] * interval).sum()
         total_energy = (df[power_col] * interval).sum()
    else:
        comm_energy = 0
        total_energy = 0

    if total_energy > 0:
        print(f"\nEnergy Analysis:")
        print(f"  Communication energy: {comm_energy:.2f}J ({comm_energy/total_energy*100:.1f}% of total)")
        print(f"  Total energy:         {total_energy:.2f}J")
    
    # Visualization (only if we have data)
    if len(df) > 0:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Timeline
        if mpi_wait_col in df.columns:
            axes[0,0].plot(df[timestamp_col], df[mpi_wait_col], label='MPI Wait %')
        
        sync_col = 'sync_overhead(%)' if 'sync_overhead(%)' in df.columns else 'sync_overhead'
        if sync_col in df.columns:
            axes[0,0].plot(df[timestamp_col], df[sync_col], label='Sync Overhead')
            
        axes[0,0].axhline(y=0.30, color='r', linestyle='--', label='MPI Threshold')
        axes[0,0].set_xlabel('Time (s)')
        axes[0,0].set_ylabel('Ratio')
        axes[0,0].set_title('Communication Indicators Over Time')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)
        
        # Phase distribution
        phase_counts = df['phase'].value_counts()
        if len(phase_counts) > 0:
            axes[0,1].pie(phase_counts, labels=phase_counts.index, autopct='%1.1f%%')
            axes[0,1].set_title('Phase Distribution')
        
        # IPC vs Power (colored by phase)
        if power_col in df.columns:
            for phase in df['phase'].unique():
                phase_data = df[df['phase'] == phase]
                axes[1,0].scatter(phase_data['ipc'], phase_data[power_col], 
                                label=phase, alpha=0.6, s=20)
            axes[1,0].set_xlabel('IPC')
            axes[1,0].set_ylabel('Power (W)')
            axes[1,0].set_title('IPC vs Power by Phase')
            axes[1,0].legend()
            axes[1,0].grid(True, alpha=0.3)
        
        # Communication metrics correlation
        if len(comm_phases) > 0 and mpi_wait_col in df.columns and sync_col in df.columns and power_col in df.columns:
            axes[1,1].scatter(comm_phases[mpi_wait_col], comm_phases[sync_col], 
                            c=comm_phases[power_col], cmap='viridis', s=30)
            axes[1,1].set_xlabel('MPI Wait %')
            axes[1,1].set_ylabel('Sync Overhead')
            axes[1,1].set_title('Communication Behavior (colored by power)')
            axes[1,1].grid(True, alpha=0.3)
            cbar = plt.colorbar(axes[1,1].collections[0], ax=axes[1,1])
            cbar.set_label('Power (W)')
        
        plt.tight_layout()
        plt.savefig('communication_analysis.png', dpi=300)
        print(f"\nVisualization saved to communication_analysis.png")
        # plt.show() # Commented out for headless environments

if __name__ == "__main__":
    analyze_communication_log()
