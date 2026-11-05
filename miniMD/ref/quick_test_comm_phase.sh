#!/bin/bash
# quick_test_comm_phase.sh
# Quick validation test (5 runs each for rapid iteration)

set -e

echo "========================================="
echo "QUICK COMMUNICATION PHASE TEST"
echo "========================================="
echo ""

# Check if we're in the right directory
if [ ! -d "../miniMD/ref" ]; then
    echo "ERROR: miniMD/ref directory not found"
    echo "Please run from project root"
    exit 1
fi

# Compile miniMD if needed
if [ ! -f "../miniMD/ref" ]; then
    echo "[BUILD] Compiling miniMD..."
    make openmpi -j 8
    cd ../..
fi

echo "[TEST] Running quick validation (5 runs each)..."
echo ""

# Run quick test
python3 system.py --mode full --runs 30

echo ""
echo "========================================="
echo "TEST COMPLETE"
echo "========================================="
echo ""
echo "Check the following files:"
echo "  - comm_phase_monitor.csv (monitoring data)"
echo "  - comm_phase_analysis.png (visualizations)"
echo "  - validation_report.json (test results)"
