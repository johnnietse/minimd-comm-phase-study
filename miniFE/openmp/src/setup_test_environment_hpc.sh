#!/bin/bash
# HPC-specific setup script for power optimization testing

echo "=== Setting up HPC Test Environment ==="

# Create necessary directories
mkdir -p results/logs
mkdir -p results/plots
mkdir -p results/reports

# Make scripts executable
chmod +x memory_bound_phase_test.sh
chmod +x serial_computation_phase_test.sh
chmod +x *.py

# Check for required tools on HPC
echo "Checking required tools on HPC node..."
command -v perf >/dev/null 2>&1 && {
    echo "✓ perf tool available"
} || {
    echo "⚠ perf not available - using RAPL counters directly"
}

# Check for RAPL counters
if [ -d "/sys/class/powercap/intel-rapl" ]; then
    echo "✓ RAPL power counters available"
else
    echo "✗ RAPL counters not found. Energy measurements may not work."
fi

# Check Python dependencies
echo "Checking Python dependencies..."
python3 -c "import matplotlib, numpy, pandas" 2>/dev/null && {
    echo "✓ Python dependencies available"
} || {
    echo "Installing Python dependencies..."
    pip3 install --user matplotlib numpy pandas
}

# Verify miniFE executable is accessible
if [ -f "../miniFE" ] || [ -f "./miniFE" ]; then
    echo "✓ miniFE executable found"
else
    echo "⚠ miniFE executable not found in current directory"
    echo "Please ensure miniFE is built and accessible"
fi

# Set up environment variables for HPC
export OMP_NUM_THREADS=32
export KMP_AFFINITY=compact

echo ""
echo "=== HPC Environment Setup Complete ==="
echo "To run memory-bound phase testing: ./memory_bound_phase_test.sh"
echo "To run serial phase testing: ./serial_computation_phase_test.sh"
echo ""
echo "Note: Make sure you're on an allocated node with:"
echo "  salloc --nodes=1 --partition=gpu-rgrant --time=30:00 --constraint=lkb"
