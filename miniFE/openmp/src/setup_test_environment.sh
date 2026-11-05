#!/bin/bash

# Setup script for memory-bound and serial phase testing

echo "Setting up test environment..."

# Create necessary directories
mkdir -p results/logs
mkdir -p results/plots
mkdir -p results/reports

# Make scripts executable
chmod +x memory_bound_phase_test.sh
chmod +x serial_computation_phase_test.sh
chmod +x *.py

# Check for required tools
echo "Checking required tools..."
command -v perf >/dev/null 2>&1 || { echo "perf not found. Please install linux-tools."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 not found. Please install Python 3."; exit 1; }

# Check Python dependencies
echo "Checking Python dependencies..."
python3 -c "import matplotlib, numpy, pandas" 2>/dev/null || {
    echo "Installing Python dependencies..."
    pip3 install matplotlib numpy pandas scipy --user
}

# Verify MiniFE executable
if [ ! -f "./miniFE" ]; then
    echo "Warning: miniFE executable not found in current directory."
    echo "Please ensure miniFE is built and available in ./miniFE"
fi

echo "Setup completed successfully!"
echo ""
echo "To run memory-bound phase testing: ./memory_bound_phase_test.sh"
echo "To run serial phase testing: ./serial_computation_phase_test.sh"
