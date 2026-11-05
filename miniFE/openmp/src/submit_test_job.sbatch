#!/bin/bash
#SBATCH --nodes=1
#SBATCH --partition=gpu-rgrant
#SBATCH --time=00:30:00
#SBATCH --constraint=lkb
#SBATCH --job-name=power_optimization_test
#SBATCH --output=test_results_%j.out

cd /path/to/power_optimization_tests

echo "Starting power optimization tests at $(date)"

# Run setup
./setup_test_environment_hpc.sh

# Run tests
echo "Running memory-bound tests..."
./memory_bound_phase_test_hpc.sh

echo "Running serial computation tests..."
./serial_computation_phase_test.sh

echo "Tests completed at $(date)"
echo "Results saved in: results/"
