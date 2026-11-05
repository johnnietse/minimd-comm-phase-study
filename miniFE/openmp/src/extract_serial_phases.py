#!/usr/bin/env python3
import re
import json
import sys

def extract_serial_phases(log_file):
    """Extract serial phase characteristics from performance log"""
    serial_phases = {}
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        # Extract single-thread performance metrics
        instructions = re.search(r'(\d+[,]?\d*)\s+instructions', content)
        cycles = re.search(r'(\d+[,]?\d*)\s+cpu-cycles', content)
        energy = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
        time = re.search(r'(\d+\.\d+)\s+seconds time elapsed', content)
        
        if all([instructions, cycles, energy, time]):
            instr_count = float(instructions.group(1).replace(',', ''))
            cycle_count = float(cycles.group(1).replace(',', ''))
            energy_consumed = float(energy.group(1))
            total_time = float(time.group(1))
            
            ipc = instr_count / cycle_count if cycle_count > 0 else 0
            power = energy_consumed / total_time if total_time > 0 else 0
            
            serial_phases = {
                'instructions': instr_count,
                'cycles': cycle_count,
                'ipc': ipc,
                'energy': energy_consumed,
                'time': total_time,
                'average_power': power,
                'cpu_utilization': 1.0,  # Assuming single-threaded
                'characteristics': {
                    'memory_bound': ipc < 1.0,
                    'compute_bound': ipc >= 1.0,
                    'energy_efficient': power < 50  # Watts
                }
            }
            
        print("Serial Phase Characteristics:")
        print(f"Instructions: {serial_phases.get('instructions', 0):,.0f}")
        print(f"Cycles: {serial_phases.get('cycles', 0):,.0f}")
        print(f"IPC: {serial_phases.get('ipc', 0):.4f}")
        print(f"Energy: {serial_phases.get('energy', 0):.2f}J")
        print(f"Time: {serial_phases.get('time', 0):.3f}s")
        print(f"Average Power: {serial_phases.get('average_power', 0):.2f}W")
        
        characteristics = serial_phases.get('characteristics', {})
        print("\nPhase Classification:")
        print(f"Memory Bound: {characteristics.get('memory_bound', False)}")
        print(f"Compute Bound: {characteristics.get('compute_bound', False)}")
        print(f"Energy Efficient: {characteristics.get('energy_efficient', False)}")
        
        # Save serial phase analysis
        with open('serial_phases.json', 'w') as f:
            json.dump(serial_phases, f, indent=2)
            
        return serial_phases
        
    except Exception as e:
        print(f"Error extracting serial phases: {e}")
        return {}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_serial_phases.py <log_file>")
        sys.exit(1)
    
    extract_serial_phases(sys.argv[1])
