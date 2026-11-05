#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import threading
import time
import os
import signal

# --- CONFIGURATION ---
DEFAULT_WORKERS = "16"
DEFAULT_RUN = "1"
CONTROLLER_WAIT = 3
CORE_FOR_CTRL = "30"

class MiniMDWindowedGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("miniMD Test Runner - ELEC 498 Group 30")
        self.root.geometry("800x600")
        self.root.configure(bg="#f0f0f0")

        self.style = ttk.Style()
        self.style.configure("TButton", font=("Helvetica", 10), padding=10)
        self.style.configure("TLabel", font=("Helvetica", 11), background="#f0f0f0")
        self.style.configure("Header.TLabel", font=("Helvetica", 16, "bold"), background="#f0f0f0", foreground="#2c3e50")

        self.is_running = False
        self.dry_run = tk.BooleanVar(value=False)

        self._setup_ui()

    def _setup_ui(self):
        # Header
        header_frame = ttk.Frame(self.root, padding=20)
        header_frame.pack(fill="x")
        ttk.Label(header_frame, text="Communication Phase Optimization GUI", style="Header.TLabel").pack()

        # Input Frame
        input_frame = ttk.LabelFrame(self.root, text="Configuration", padding=15)
        input_frame.pack(padx=20, pady=10, fill="x")

        ttk.Label(input_frame, text="NUM_WORKERS (MPI Count):").grid(row=0, column=0, sticky="w", pady=5)
        self.workers_entry = ttk.Entry(input_frame, width=10)
        self.workers_entry.insert(0, DEFAULT_WORKERS)
        self.workers_entry.grid(row=0, column=1, sticky="w", padx=10)

        ttk.Label(input_frame, text="RUN ID:").grid(row=0, column=2, sticky="w", pady=5, padx=(20, 0))
        self.run_entry = ttk.Entry(input_frame, width=10)
        self.run_entry.insert(0, DEFAULT_RUN)
        self.run_entry.grid(row=0, column=3, sticky="w", padx=10)

        ttk.Checkbutton(input_frame, text="Dry Run Mode (Simulated)", variable=self.dry_run).grid(row=0, column=4, padx=20)

        # Action Buttons
        button_frame = ttk.Frame(self.root, padding=20)
        button_frame.pack(fill="x")

        self.btn_b = ttk.Button(button_frame, text="Run Test B (No Controller)", command=lambda: self._start_test("B"))
        self.btn_b.pack(side="left", padx=5, expand=True, fill="x")

        self.btn_c = ttk.Button(button_frame, text="Run Test C (Comm Controller)", command=lambda: self._start_test("C"))
        self.btn_c.pack(side="left", padx=5, expand=True, fill="x")

        self.btn_c2 = ttk.Button(button_frame, text="Run Test C2 (Integrated)", command=lambda: self._start_test("C2"))
        self.btn_c2.pack(side="left", padx=5, expand=True, fill="x")

        # Console Output
        console_frame = ttk.LabelFrame(self.root, text="Live Output Log", padding=10)
        console_frame.pack(padx=20, pady=10, fill="both", expand=True)

        self.log_area = scrolledtext.ScrolledText(console_frame, height=15, font=("Consolas", 10), bg="#1e1e1e", fg="#00ff00")
        self.log_area.pack(fill="both", expand=True)

        # Status Bar
        self.status_var = tk.StringVar(value="Ready")
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief="sunken", anchor="w", padding=5)
        self.status_bar.pack(side="bottom", fill="x")

    def log(self, text, color=None):
        self.log_area.insert(tk.END, f"[{time.strftime('%H:%M:%S')}] {text}\n")
        self.log_area.see(tk.END)

    def _start_test(self, test_type):
        if self.is_running:
            messagebox.showwarning("Busy", "A test is already running!")
            return

        workers = self.workers_entry.get()
        run_id = self.run_entry.get()

        if not workers.isdigit() or not run_id.isdigit():
            messagebox.showerror("Error", "Workers and Run ID must be numbers!")
            return

        self.is_running = True
        self.status_var.set(f"Executing Test {test_type}...")
        self.log(f"--- STARTING TEST {test_type} (Workers: {workers}, Run: {run_id}) ---")
        
        # Run in separate thread to prevent GUI freezing
        thread = threading.Thread(target=self._test_thread, args=(test_type, workers, run_id))
        thread.start()

    def _get_energy(self):
        if self.dry_run.get(): return 1000, 100000000
        try:
            e = int(open('/sys/class/powercap/intel-rapl:0/energy_uj').read().strip())
            m = int(open('/sys/class/powercap/intel-rapl:0/max_energy_range_uj').read().strip())
            return e, m
        except: return 0, 1

    def _test_thread(self, test_type, workers, run_id):
        try:
            if not self.dry_run.get():
                # Cleanup and Governor Reset
                self.log("Resetting governors and killing old controllers...")
                subprocess.run("pkill -f freq_controller", shell=True)
                reset_cmd = f"for c in $(seq 0 $(( {workers} - 1 ))); do echo 'performance' > /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor; done"
                subprocess.run(reset_cmd, shell=True)
                subprocess.run("rm -f phase_marker.txt", shell=True)

            before_e, max_r = self._get_energy()

            # Launch Controller if Test C/C2
            if test_type in ["C", "C2"]:
                script = "comm_freq_controller.py" if test_type == "C" else "integrated_freq_controller.py"
                log_tag = "c" if test_type == "C" else "c2"
                ctrl_log = f"/tmp/ctrl_{log_tag}_{workers}_{run_id}.log"
                self.log(f"Starting {script}...")
                
                if not self.dry_run.get():
                    ctrl_cmd = f"taskset -c {CORE_FOR_CTRL} python3 -u {script} --workers {workers} > {ctrl_log} 2>&1 &"
                    subprocess.Popen(ctrl_cmd, shell=True)
                    time.sleep(CONTROLLER_WAIT)

            # Run miniMD
            app_tag = test_type.lower()
            app_log = f"/tmp/test_{app_tag}_{workers}_{run_id}.log"
            self.log(f"Executing miniMD (Logging to {app_log})...")
            
            if self.dry_run.get():
                time.sleep(3) # simulate work
            else:
                app_cmd = f"mpirun -np {workers} --bind-to core ./miniMD_openmpi -i in.lj.miniMD 2>&1 | tee {app_log}"
                subprocess.run(app_cmd, shell=True)

            after_e, _ = self._get_energy()
            
            # Energy Calculation
            diff = after_e - before_e
            if diff < 0: diff += max_r
            energy_j = round(diff / 1000000.0, 3)

            # Cleanup
            if test_type in ["C", "C2"] and not self.dry_run.get():
                subprocess.run("pkill -f freq_controller", shell=True)
                reset_cmd = f"for c in $(seq 0 $(( {workers} - 1 ))); do echo 'performance' > /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor; done"
                subprocess.run(reset_cmd, shell=True)

            self.log(f"TEST {test_type} COMPLETE.", color="green")
            self.log(f"Measured Energy: {energy_j} J")
            self.status_var.set("Ready")

        except Exception as e:
            self.log(f"ERROR: {str(e)}")
            self.status_var.set("Error Occurred")
        finally:
            self.is_running = False

if __name__ == "__main__":
    root = tk.Tk()
    app = MiniMDWindowedGUI(root)
    
    def on_closing():
        subprocess.run("pkill -f freq_controller", shell=True)
        root.destroy()
        
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()
