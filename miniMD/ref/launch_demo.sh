#!/bin/bash

# Define the shared memory file to ensure all tools look at the exact same location
export PHASE_HINT_PATH=/dev/shm/minimd_phase_hints_myrun.bin

# Kill any existing session with this name to avoid clashing
tmux kill-session -t minimd_demo 2>/dev/null

# Create a new tmux session in detached mode (Pane 0 will be the Monitor)
tmux new-session -d -s minimd_demo

# --- PANE 0 (Top): Monitor (mon.py) ---
tmux send-keys -t minimd_demo:0.0 "export PHASE_HINT_PATH=$PHASE_HINT_PATH" C-m
tmux send-keys -t minimd_demo:0.0 "taskset -c 30 python3 -u ./mon.py --hint-file \"\$PHASE_HINT_PATH\" --freq-low 1200000 --freq-mid 1600000 --poll-ms 2 --low-after-ms 2 --mid-after-ms 5" C-m

# Split the monitor pane vertically (creating a new pane 1 at the bottom)
tmux split-window -v -t minimd_demo:0.0

# --- PANE 1 (Bottom Left): mpirun execution ---
tmux send-keys -t minimd_demo:0.1 "export PHASE_HINT_PATH=$PHASE_HINT_PATH" C-m
# We type out the command but don't hit enter yet, so you can see it before it runs
tmux send-keys -t minimd_demo:0.1 "OMP_NUM_THREADS=1 OMP_PROC_BIND=true OMP_PLACES=cores taskset -c 4-29 mpirun -np 26 -x PHASE_HINT_PATH --bind-to core --map-by core --report-bindings ./miniMD_openmpi -i in.lj.miniMD"

# Split the bottom pane horizontally, strongly favoring the dashboard with enough width (130 cells) to fit the traditional strings
tmux split-window -h -l 130 -t minimd_demo:0.1

# --- PANE 2 (Bottom Right): Dashboard ---
tmux send-keys -t minimd_demo:0.2 "export PHASE_HINT_PATH=$PHASE_HINT_PATH" C-m
tmux send-keys -t minimd_demo:0.2 "python3 dashboard.py --cores 32 --refresh 0.5 --hint-file \"\$PHASE_HINT_PATH\"" C-m

# Keep top pane extremely short specifically to preserve vertical grid height for the dashboard
tmux resize-pane -t minimd_demo:0.0 -y 4

# Start mpirun last (now we hit enter) so the monitor and dashboard are already waiting for it
tmux send-keys -t minimd_demo:0.1 C-m

# Attach to the session!
tmux attach-session -t minimd_demo
