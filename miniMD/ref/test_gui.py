#!/usr/bin/env python3
import os
import subprocess
import time
import sys
import signal

# --- CONFIGURATION ---
DEFAULT_WORKERS = 16
DEFAULT_RUN = 1
CONTROLLER_WAIT = 3
CORE_FOR_CTRL = "30"

# ANSI Colors for "Visually Pleasing" TUI
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}")
    print(f" {text}")
    print(f"{'='*60}{Colors.ENDC}")

def log_info(text):
    print(f"{Colors.OKCYAN}[INFO]{Colors.ENDC} {text}")

def log_success(text):
    print(f"{Colors.OKGREEN}[SUCCESS]{Colors.ENDC} {text}")

def log_warn(text):
    print(f"{Colors.WARNING}[WARN]{Colors.ENDC} {text}")

def log_error(text):
    print(f"{Colors.FAIL}[ERROR]{Colors.ENDC} {text}")

class TestRunner:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.num_workers = DEFAULT_WORKERS
        self.run_id = DEFAULT_RUN

    def run_cmd(self, cmd, shell=True, capture=False):
        if self.dry_run:
            print(f"{Colors.OKBLUE}[DRY RUN]{Colors.ENDC} Executing: {cmd}")
            return "PERF_SUMMARY: 1.0 1.0 1.0 1.0 1.0 1.0" if capture else None
        
        try:
            if capture:
                result = subprocess.check_output(cmd, shell=shell, stderr=subprocess.STDOUT)
                return result.decode('utf-8')
            else:
                subprocess.run(cmd, shell=shell, check=True)
                return None
        except subprocess.CalledProcessError as e:
            log_error(f"Command failed: {cmd}")
            if capture:
                return e.output.decode('utf-8')
            return None

    def reset_governors(self, workers):
        log_info(f"Resetting governors to 'performance' for {workers} cores...")
        cmd = f"for c in $(seq 0 $(( {workers} - 1 ))); do echo 'performance' > /sys/devices/system/cpu/cpu$c/cpufreq/scaling_governor; done"
        self.run_cmd(cmd)

    def parse_perf_summary(self, log_path):
        if self.dry_run:
            return "1.0,1.0,1.0,1.0,1.0,1.0"
        
        try:
            with open(log_path, 'r') as f:
                for line in f:
                    if "PERF_SUMMARY" in line:
                        parts = line.split()
                        # Extract columns 5, 6, 7, 8, 9, 10
                        if len(parts) >= 11:
                            return ",".join(parts[4:10])
            return "0,0,0,0,0,0"
        except Exception as e:
            log_error(f"Failed to parse timing from {log_path}: {e}")
            return "0,0,0,0,0,0"

    def get_energy(self):
        if self.dry_run:
            return 100.0, 100000000 # dummy energy and max range
        
        try:
            energy = int(open('/sys/class/powercap/intel-rapl:0/energy_uj').read().strip())
            max_range = int(open('/sys/class/powercap/intel-rapl:0/max_energy_range_uj').read().strip())
            return energy, max_range
        except Exception as e:
            log_error(f"Failed to read energy: {e}")
            return 0, 1

    def calculate_energy(self, before, after, max_range):
        diff = after - before
        if diff < 0:
            diff += max_range
        return round(diff / 1000000.0, 3)

    def test_b(self, workers, run_id):
        print_header(f"RUNNING TEST B (Comm Phase ON, No Controller) - Run {run_id}")
        
        self.reset_governors(workers)
        self.run_cmd("rm -f phase_marker.txt")
        
        before_e, max_r = self.get_energy()
        
        log_path = f"/tmp/test_b_{workers}_{run_id}.log"
        log_info(f"Executing miniMD. Logging to {log_path}...")
        cmd = f"mpirun -np {workers} --bind-to core ./miniMD_openmpi -i in.lj.miniMD 2>&1 | tee {log_path}"
        self.run_cmd(cmd)
        
        after_e, _ = self.get_energy()
        energy_j = self.calculate_energy(before_e, after_e, max_r)
        
        log_success(f"Test B complete. Energy: {energy_j} J")
        
        perf_data = self.parse_perf_summary(log_path)
        csv_line = f"{run_id},{workers},{energy_j},{perf_data}\n"
        
        if not self.dry_run:
            with open("results_manual_test_b.csv", "a") as f:
                f.write(csv_line)
            log_info(f"Appended to results_manual_test_b.csv")

    def test_c_shared(self, workers, run_id, controller_script, test_label, csv_name, log_prefix):
        print_header(f"RUNNING {test_label} (Comm Phase ON + {controller_script}) - Run {run_id}")
        
        self.run_cmd("pkill -f freq_controller", shell=True)
        self.reset_governors(workers)
        self.run_cmd("rm -f phase_marker.txt")
        
        ctrl_log = f"/tmp/ctrl_{log_prefix}_{workers}_{run_id}.log"
        log_info(f"Starting controller {controller_script} in background...")
        ctrl_cmd = f"taskset -c {CORE_FOR_CTRL} python3 -u {controller_script} --workers {workers} > {ctrl_log} 2>&1 &"
        
        if self.dry_run:
            print(f"{Colors.OKBLUE}[DRY RUN]{Colors.ENDC} Starting controller...")
        else:
            subprocess.Popen(ctrl_cmd, shell=True)
            time.sleep(CONTROLLER_WAIT)
            
        before_e, max_r = self.get_energy()
        
        app_log = f"/tmp/test_{log_prefix}_{workers}_{run_id}.log"
        log_info(f"Executing miniMD. Logging to {app_log}...")
        app_cmd = f"mpirun -np {workers} --bind-to core ./miniMD_openmpi -i in.lj.miniMD 2>&1 | tee {app_log}"
        self.run_cmd(app_cmd)
        
        after_e, _ = self.get_energy()
        energy_j = self.calculate_energy(before_e, after_e, max_r)
        
        log_info("Stopping controller...")
        self.run_cmd("pkill -f freq_controller", shell=True)
        
        # Count transitions
        if self.dry_run:
            transitions = 10
        else:
            grep_pattern = "Phase transition:" if log_prefix == "c" else "(Phase transition:|PHASE:)"
            trans_cmd = f"grep -cE '{grep_pattern}' {ctrl_log}"
            try:
                transitions = subprocess.check_output(trans_cmd, shell=True).decode('utf-8').strip()
            except:
                transitions = "0"
        
        log_success(f"{test_label} complete. Energy: {energy_j} J, Transitions: {transitions}")
        
        perf_data = self.parse_perf_summary(app_log)
        csv_line = f"{run_id},{workers},{energy_j},{perf_data},{transitions}\n"
        
        if not self.dry_run:
            with open(csv_name, "a") as f:
                f.write(csv_line)
            log_info(f"Appended to {csv_name}")
            
        self.reset_governors(workers)

def main_menu():
    runner = TestRunner(dry_run=False)
    
    while True:
        os.system('clear')
        print(f"{Colors.BOLD}{Colors.OKBLUE}============================================================")
        print("          miniMD COMMUNICATION PHASE TEST RUNNER           ")
        print(f"============================================================{Colors.ENDC}")
        print(f" Current Settings: Workers={Colors.BOLD}{runner.num_workers}{Colors.ENDC}, Run ID={Colors.BOLD}{runner.run_id}{Colors.ENDC}")
        print(f" Mode: {Colors.OKGREEN if not runner.dry_run else Colors.WARNING}{'LIVE RUN' if not runner.dry_run else 'DRY RUN'}{Colors.ENDC}")
        print("------------------------------------------------------------")
        print(f"{Colors.BOLD}1.{Colors.ENDC} Run Test B (Comm Phase ON, No Controller)")
        print(f"{Colors.BOLD}2.{Colors.ENDC} Run Test C (Comm Phase ON + comm_freq_controller.py)")
        print(f"{Colors.BOLD}3.{Colors.ENDC} Run Test C2 (Comm Phase ON + integrated_freq_controller.py)")
        print("------------------------------------------------------------")
        print(f"{Colors.BOLD}4.{Colors.ENDC} Set NUM_WORKERS (Current: {runner.num_workers})")
        print(f"{Colors.BOLD}5.{Colors.ENDC} Set RUN ID (Current: {runner.run_id})")
        print(f"{Colors.BOLD}d.{Colors.ENDC} Toggle Dry Run (Current: {'ON' if runner.dry_run else 'OFF'})")
        print(f"{Colors.BOLD}q.{Colors.ENDC} Quit")
        print("------------------------------------------------------------")
        
        choice = input(f"{Colors.BOLD}Select an option: {Colors.ENDC}").lower()
        
        if choice == '1':
            runner.test_b(runner.num_workers, runner.run_id)
            input("\nPress Enter to return to menu...")
        elif choice == '2':
            runner.test_c_shared(runner.num_workers, runner.run_id, "comm_freq_controller.py", "TEST C", "results_manual_test_c.csv", "c")
            input("\nPress Enter to return to menu...")
        elif choice == '3':
            runner.test_c_shared(runner.num_workers, runner.run_id, "integrated_freq_controller.py", "TEST C2", "results_manual_test_c2.csv", "c2")
            input("\nPress Enter to return to menu...")
        elif choice == '4':
            try:
                new_w = int(input("Enter NUM_WORKERS: "))
                runner.num_workers = new_w
            except: log_error("Invalid input.")
        elif choice == '5':
            try:
                new_r = int(input("Enter RUN ID: "))
                runner.run_id = new_r
            except: log_error("Invalid input.")
        elif choice == 'd':
            runner.dry_run = not runner.dry_run
        elif choice == 'q':
            log_info("Goodbye!")
            break
        else:
            log_error("Invalid choice.")
            time.sleep(1)

if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print(f"\n{Colors.WARNING}Interrupted! Cleaning up...{Colors.ENDC}")
        subprocess.run("pkill -f freq_controller", shell=True)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main_menu()
