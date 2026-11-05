#!/bin/bash
# Memory-bound phase test script for MiniFE
# Use large problem sizes to systematically stress the memory hierarchy

# Configure environment for memory-intensive workload
export OMP_NUM_THREADS=32
export KMP_AFFINITY=compact

echo "=== Memory-Bound Phase Testing ==="
echo "Starting baseline measurement..."

# Baseline measurement for memory-bound phase
perf stat -e power/energy-pkg/,power/energy-ram/,cache-misses,cache-references,instructions,cpu-cycles -a -o memory_baseline.log \
./miniFE -i input_memory_large.in -c config_memory.cfg

echo "Baseline measurement completed."
echo "Extracting memory access patterns..."

# Extract memory access patterns from baseline
python3 extract_memory_patterns.py memory_baseline.log

echo "Running optimized memory-bound phase with power management..."

# Optimized run with memory-aware power management
perf stat -e power/energy-pkg/,power/energy-ram/,cache-misses,cache-references,instructions,cpu-cycles -a -o memory_optimized.log \
./miniFE -i input_memory_large.in -c config_memory.cfg --power-optimize memory

echo "Analyzing memory access patterns and power consumption..."

# Analyze memory access patterns and power consumption
python3 memory_analysis.py memory_baseline.log memory_optimized.log

# Test across multiple problem sizes to quantify relationship
echo "Testing across multiple problem sizes..."
for size in 1000 5000 10000 50000 100000; do
    echo "Testing problem size: $size"
    perf stat -e power/energy-pkg/,power/energy-ram/,cache-misses,cache-references -a -o memory_size_${size}.log \
    ./miniFE -i input_memory_${size}.in -c config_memory.cfg --power-optimize memory
done

echo "Generating comprehensive memory-bound analysis report..."

# Generate comprehensive memory-bound analysis report
python3 memory_comprehensive_analysis.py memory_baseline.log memory_optimized.log memory_size_*.log

echo "Memory-bound phase test completed. Reports generated."

# End of memory_bound_phase_test.sh


