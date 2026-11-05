#!/bin/bash
# ================================================================
# CAPSTONE DEMO SETUP: Unified Run Environment
# ================================================================
# Creates a clean 'capstone_run' folder with everything pre-configured.
# No 'nano' required.
# ================================================================

TARGET_DIR="capstone_run"

echo "Creating unified run directory: $TARGET_DIR..."
mkdir -p "$TARGET_DIR"

# 1. From Zane's MPI Comm (Dashboard and Monitor)
echo "Pulling Dashboard and Monitor from zane_mpi_comm..."
cp zane_mpi_comm/mpi_comm/mon.py "$TARGET_DIR/monitor.py"
cp zane_mpi_comm/mpi_comm/dashboard.py "$TARGET_DIR/dvfs_dashboard.py"
cp zane_mpi_comm/mpi_comm/bridge_to_dashboard.py "$TARGET_DIR/"

# 2. From Johnnie's Comm Phase (Your latest MPI Binary and Input)
echo "Pulling miniMD Binary and Input from johnnie-comm-phase..."
cp johnnie-comm-phase/miniMD_openmpi "$TARGET_DIR/" 2>/dev/null || echo "Warning: miniMD_openmpi not found in johnnie-comm-phase."
cp johnnie-comm-phase/in.lj.miniMD "$TARGET_DIR/" 2>/dev/null || echo "Warning: in.lj.miniMD not found in johnnie-comm-phase."

# 3. Create a quick helper for the dashboard hint path
echo "Configuring environment..."
cat > "$TARGET_DIR/how_to_run.txt" << EOF
================================================================
CAPSTONE DEMO: One-Click Run Guide (No Nano Needed)
================================================================

TERMINAL 1: Launch the Dashboard
--------------------------------
cd $TARGET_DIR
python3 dvfs_dashboard.py --cores 32

TERMINAL 2: Run your MPI Code (Johnnie's code)
----------------------------------------------
cd $TARGET_DIR
# Start the bridge (connects text output to dashboard)
python3 bridge_to_dashboard.py --ranks 16 &

# Run miniMD
mpirun -np 16 --bind-to core ./miniMD_openmpi -i in.lj.miniMD

TERMINAL 3: Manual Controller (Optional)
----------------------------------------
cd $TARGET_DIR
# If using Zane's monitor:
python3 monitor.py --freq-low 1200000 --poll-ms 2
EOF

# Make everything executable
chmod +x "$TARGET_DIR"/*.py 2>/dev/null || true

echo "================================================================"
echo "SUCCESS: Folder '$TARGET_DIR' is ready for export to the cluster."
echo "Check '$TARGET_DIR/how_to_run.txt' for instructions."
echo "================================================================"
