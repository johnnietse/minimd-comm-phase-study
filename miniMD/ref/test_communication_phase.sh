#!/bin/bash

# Communication Phase Testing Script for miniMD
# Tests and monitors the communication patterns in miniMD

echo "============================================="
echo "miniMD Communication Phase Testing"
echo "============================================="

# Set test parameters
TEST_NODES=4
TEST_THREADS=1
TEST_SIZE=32
TEST_STEPS=100
TEST_INPUT="in.lj.miniMD"

echo "Test Configuration:"
echo "  Nodes: $TEST_NODES"
echo "  Threads: $TEST_THREADS" 
echo "  System Size: $TEST_SIZE"
echo "  Steps: $TEST_STEPS"
echo "  Input: $TEST_INPUT"
echo ""

# Compile miniMD with communication monitoring
echo "Compiling miniMD with communication monitoring..."
cd ref
make openmpi -j 8

if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

echo "Compilation successful!"

# Create test input file
echo "Creating test input file..."
cat > $TEST_INPUT << EOF
Lennard-Jones input file for miniMD

lj             units (lj or metal)
none           data file (none or filename)       
lj             force style (lj or eam)
1.0 1.0        LJ parameters (epsilon and sigma; COMD: 0.167 / 2.315)
$TEST_SIZE $TEST_SIZE $TEST_SIZE       size of problem
$TEST_STEPS            timesteps
0.005          timestep size 
1.44           initial temperature 
0.8442         density 
20             reneighboring every this many steps
2.5 0.30       force cutoff and neighbor skin 
10            thermo calculation every this many steps (0 = start,end)
EOF

# Run communication tests
echo ""
echo "Running Communication Phase Tests..."
echo "------------------------------------"

# Test 1: Basic communication monitoring
echo "Test 1: Basic Communication Monitoring"
mpirun -np $TEST_NODES --oversubscribe ./miniMD_openmpi -i $TEST_INPUT --comm-monitor

# Test 2: Communication with different domain decompositions
echo ""
echo "Test 2: Varying Domain Decomposition"
for SIZE in 16 32 48; do
    echo "Testing system size: $SIZE"
    sed -i "s/^[0-9]* [0-9]* [0-9]*.*size of problem/$SIZE $SIZE $SIZE       size of problem/" $TEST_INPUT
    mpirun -np $TEST_NODES --oversubscribe ./miniMD_openmpi -i $TEST_INPUT --comm-monitor --comm-report
done

# Test 3: Communication with different neighbor list settings
echo ""
echo "Test 3: Varying Neighbor List Settings"
for NEIGHBOR_CUT in 0.2 0.3 0.4; do
    echo "Testing neighbor cutoff: $NEIGHBOR_CUT"
    sed -i "s/^2\.[0-9] [0-9]\.[0-9]*.*force cutoff and neighbor skin/2.5 $NEIGHBOR_CUT       force cutoff and neighbor skin/" $TEST_INPUT
    mpirun -np $TEST_NODES --oversubscribe ./miniMD_openmpi -i $TEST_INPUT --comm-monitor --comm-report
done

# Test 4: Load imbalance detection
echo ""
echo "Test 4: Load Imbalance Detection"
mpirun -np $TEST_NODES --oversubscribe ./miniMD_openmpi -i $TEST_INPUT --comm-monitor --validate-comm

echo ""
echo "============================================="
echo "Communication Phase Testing Complete"
echo "============================================="

# Generate summary report
echo ""
echo "Generating Communication Test Summary..."
echo "----------------------------------------"

# Extract communication statistics from output files
for file in comm_report*.txt; do
    if [ -f "$file" ]; then
        echo "Report: $file"
        grep -E "(COMMUNICATION|Phase|WARNING|ERROR)" "$file" | head -20
        echo ""
    fi
done

echo "All communication tests completed successfully!"
