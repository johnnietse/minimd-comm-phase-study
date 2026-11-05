#!/bin/bash
echo "=== Verifying HPC Test Setup ==="

# Check node allocation
echo "1. Checking node allocation..."
if [ -n "$SLURM_JOB_ID" ]; then
    echo "✓ Running on allocated node: $SLURM_JOB_ID"
else
    echo "✗ Not running on allocated node. Please run:"
    echo "  salloc --nodes=1 --partition=gpu-rgrant --time=30:00 --constraint=lkb"
    exit 1
fi

# Check RAPL counters
echo "2. Checking RAPL counters..."
if [ -d "/sys/class/powercap/intel-rapl" ]; then
    echo "✓ RAPL counters available"
    echo "  Package power limit: $(cat /sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw 2>/dev/null || echo "N/A") μW"
    echo "  Current energy: $(cat /sys/class/powercap/intel-rapl:0/energy_uj 2>/dev/null || echo "N/A") μJ"
else
    echo "✗ RAPL counters not available"
fi

# Check miniFE
echo "3. Checking miniFE..."
if [ -f "../miniFE" ]; then
    echo "✓ miniFE found at ../miniFE"
elif [ -f "./miniFE" ]; then
    echo "✓ miniFE found at ./miniFE"
else
    echo "✗ miniFE not found. Please compile miniFE first."
    echo "  cd ../miniFE/openmp && make"
fi

# Check Python
echo "4. Checking Python dependencies..."
python3 -c "import matplotlib, numpy, pandas" 2>/dev/null && {
    echo "✓ Python dependencies available"
} || {
    echo "⚠ Installing Python dependencies..."
    pip3 install --user matplotlib numpy pandas
}

echo ""
echo "=== Setup Verification Complete ==="
echo "Run './memory_bound_phase_test_hpc.sh' to start testing"
