
#!/bin/bash
# HPC-optimized memory-bound phase test using perf and RAPL


echo "=== Memory-Bound Phase Testing on HPC ==="

mpirun ./miniFE.x -nx 64 --ny 64 --nz 64 --num_steps 10

max_iterations = 500
tolerance = 1.0e-10

# Environment setup
export OMP_NUM_THREADS=8
export KMP_AFFINITY=compact

MINIFE_EXEC="./miniFE.x"
INPUT_FILE="input_memory_5000.in"
CONFIG_FILE="config_memory.cfg"

# Check executable
if [ ! -f "$MINIFE_EXEC" ]; then
    echo "Error: miniFE executable not found!"
    exit 1
fi

run_perf_test() {
    local test_name=$1
    local command=$2
    local output_log=$3

    echo "Starting $test_name..."
    start_time=$(date +%s.%N)

    eval "$command"

    end_time=$(date +%s.%N)
    runtime=$(awk "BEGIN {print $end_time - $start_time}")

    # runtime=$( { /usr/bin/time -f "%e" eval "$command"; } 2>&1 )

    echo "Runtime: $runtime seconds" >> "$output_log"
    echo "Completed $test_name"
}


# Baseline
run_perf_test "Memory-Bound Baseline" \
    "perf stat -e power/energy-pkg/,power/energy-ram/,cache-references,cache-misses,instructions,cpu-cycles $MINIFE_EXEC -i $INPUT_FILE -c $CONFIG_FILE --problem-size 5000 2> memory_baseline.log" \
    "memory_baseline.log"

# Optimized
run_perf_test "Memory-Bound Optimized" \
    "perf stat -e power/energy-pkg/,power/energy-ram/,cache-references,cache-misses,instructions,cpu-cycles $MINIFE_EXEC -i $INPUT_FILE -c $CONFIG_FILE --power-optimize memory --problem-size 5000 2> memory_optimized.log" \
    "memory_optimized.log"




# Analysis
if [ -f memory_analysis.py ]; then
    echo "Analyzing results..."
    python3 memory_analysis.py memory_baseline.log memory_optimized.log
else
    echo "Warning: memory_analysis.py not found, skipping analysis"
fi


# Test across multiple problem sizes to quantify relationship
echo "Testing across multiple problem sizes..."
for size in 1000 5000 10000 50000 100000; do
    perf stat -e power/energy-pkg/,power/energy-ram/,cache-misses,cache-references -a -o memory_size_${size}.log \
    ./miniFE -i input_memory_${size}.in -c config_memory.cfg --power-optimize memory
done

# Generate comprehensive memory-bound analysis report
python memory_comprehensive_analysis.py memory_baseline.log memory_optimized.log memory_size_*.log

echo "Memory-bound phase testing completed!"
