#!/usr/bin/env python3
import re
import json
import sys

def extract_memory_patterns(log_file):
    """Extract memory access patterns from perf log"""
    patterns = {}
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        # Extract cache statistics
        cache_misses = re.search(r'(\d+[,]?\d*)\s+cache-misses', content)
        cache_refs = re.search(r'(\d+[,]?\d*)\s+cache-references', content)
        
        if cache_misses and cache_refs:
            misses = float(cache_misses.group(1).replace(',', ''))
            refs = float(cache_refs.group(1).replace(',', ''))
            miss_rate = (misses / refs * 100) if refs > 0 else 0
            
            patterns['cache_misses'] = misses
            patterns['cache_references'] = refs
            patterns['cache_miss_rate'] = miss_rate
            
        # Extract memory bandwidth indicators
        instructions = re.search(r'(\d+[,]?\d*)\s+instructions', content)
        cycles = re.search(r'(\d+[,]?\d*)\s+cpu-cycles', content)
        
        if instructions and cycles:
            instr_count = float(instructions.group(1).replace(',', ''))
            cycle_count = float(cycles.group(1).replace(',', ''))
            ipc = instr_count / cycle_count if cycle_count > 0 else 0
            
            patterns['instructions'] = instr_count
            patterns['cycles'] = cycle_count
            patterns['ipc'] = ipc
            
        # Extract energy consumption
        energy_pkg = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-pkg/', content)
        energy_ram = re.search(r'(\d+\.?\d*)\s+Joules\s+power/energy-ram/', content)
        
        if energy_pkg:
            patterns['energy_pkg'] = float(energy_pkg.group(1))
        if energy_ram:
            patterns['energy_ram'] = float(energy_ram.group(1))
            
        # Classify memory intensity
        if patterns.get('cache_miss_rate', 0) > 10:
            patterns['memory_intensity'] = 'High'
        elif patterns.get('cache_miss_rate', 0) > 5:
            patterns['memory_intensity'] = 'Medium'
        else:
            patterns['memory_intensity'] = 'Low'
            
        print("Memory Access Patterns Extracted:")
        print(f"Cache Miss Rate: {patterns.get('cache_miss_rate', 0):.2f}%")
        print(f"IPC: {patterns.get('ipc', 0):.4f}")
        print(f"Memory Intensity: {patterns.get('memory_intensity', 'Unknown')}")
        print(f"Package Energy: {patterns.get('energy_pkg', 0):.2f} J")
        print(f"RAM Energy: {patterns.get('energy_ram', 0):.2f} J")
        
        # Save patterns to file
        with open('memory_patterns.json', 'w') as f:
            json.dump(patterns, f, indent=2)
            
        return patterns
        
    except Exception as e:
        print(f"Error extracting memory patterns: {e}")
        return {}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_memory_patterns.py <log_file>")
        sys.exit(1)
    
    extract_memory_patterns(sys.argv[1])
