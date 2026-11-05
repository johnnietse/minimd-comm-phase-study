#!/bin/bash
# Serial computation phase test script for MiniFE
# Focus on initialization and finalization phases with single-threaded execution

# Configure for single-threaded serial execution
export OMP_NUM_THREADS=1
export KMP_AFFINITY=compact

mpirun ./miniFE.x --nx 32 --ny 32 --nx 32 --num_steps 10

echo "=== Serial Computation Phase Testing ==="
echo "Starting baseline serial phase measurement..."

# Baseline serial phase measurement
perf stat -e power/energy-pkg/,power/energy-ram/,instructions,cpu-cycles -a -o serial_baseline.log \
./miniFE -i input_serial.in -c config_serial.cfg

echo "Baseline measurement completed."
echo "Measuring individual phase timing and energy consumption..."

# Measure individual phase timing and energy consumption
python3 phase_analysis.py serial_baseline.log

echo "Extracting serial phase boundaries and characteristics..."

# Extract serial phase boundaries and characteristics
python3 extract_serial_phases.py serial_baseline.log

echo "Running optimized serial phase with power management..."

# Test serial phase optimization with aggressive power reduction
perf stat -e power/energy-pkg/,power/energy-ram/,instructions,cpu-cycles -a -o serial_optimized.log \
./miniFE -i input_serial.in -c config_serial.cfg --power-optimize serial

echo "Validating no performance regression in serial phases..."

# Validate no performance regression in serial phases
python3 validate_serial_performance.py serial_baseline.log serial_optimized.log

echo "Testing power-gating on unused cores during serial execution..."

# Test power-gating on unused cores during serial execution
perf stat -e power/energy-pkg/,power/energy-ram/ -a -o serial_power_gated.log \
./miniFE -i input_serial.in -c config_serial.cfg --power-optimize serial --power-gate-unused

echo "Analyzing single-thread performance and energy efficiency..."

# Analyze single-thread performance and energy efficiency
python3 serial_analysis.py serial_baseline.log serial_optimized.log serial_power_gated.log

echo "Generating serial phase optimization report..."

# Generate serial phase optimization report
python3 serial_optimization_report.py serial_baseline.log serial_optimized.log

echo "Serial computation phase testing completed!"

# End of serial_computation_phase_test.sh

