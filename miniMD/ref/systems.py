#!/usr/bin/env python3
"""
Comprehensive Testing Framework for Communication Phase Monitoring
Includes automated testing, validation, and comparison against baseline
"""

import subprocess
import json
import time
import statistics
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
from scipy import stats

class CommunicationPhaseValidator:
    """Validates communication phase optimization against blueprint requirements"""
    
    def __init__(self):
        self.test_results = []
        self.baseline_data = {}
        self.optimized_data = {}
        
    def run_baseline_test(self, num_runs: int = 20) -> Dict:
        """
        Run baseline tests without optimization
        Blueprint requirement: 20 runs for statistical significance
        """
        print("="*70)
        print("RUNNING BASELINE TESTS (NO OPTIMIZATION)")
        print("="*70)
        
        energy_samples = []
        runtime_samples = []
        
        for run in range(num_runs):
            print(f"\n[Baseline Run {run+1}/{num_runs}]")
            
            # Run miniMD without monitoring
            start_time = time.time()
            
            cmd = [
                'mpirun', '--oversubscribe', '-np', '8',
                './miniMD_openmpi', '-i', 'in.lj.miniMD', '-n', '2000'
            ]
            
            # Measure energy with perf
            perf_cmd = [
                'perf', 'stat', '-a',
                '-e', 'power/energy-pkg/',
                '-e', 'power/energy-ram/',
                '--', *cmd
            ]
            
            result = subprocess.run(
                perf_cmd,
                capture_output=True,
                text=True,
                cwd='miniMD/ref'
            )
            
            runtime = time.time() - start_time
            
            # Parse energy from perf output
            energy_pkg = self._parse_perf_energy(result.stderr, 'energy-pkg')
            energy_ram = self._parse_perf_energy(result.stderr, 'energy-ram')
            total_energy = energy_pkg + energy_ram
            
            energy_samples.append(total_energy)
            runtime_samples.append(runtime)
            
            print(f"  Energy: {total_energy:.2f} J, Runtime: {runtime:.2f} s")
            
            # Cool down between runs
            time.sleep(2)
        
        self.baseline_data = {
            'energy': energy_samples,
            'runtime': runtime_samples,
            'mean_energy': statistics.mean(energy_samples),
            'std_energy': statistics.stdev(energy_samples),
            'mean_runtime': statistics.mean(runtime_samples),
            'std_runtime': statistics.stdev(runtime_samples)
        }
        
        print("\n" + "="*70)
        print("BASELINE RESULTS")
        print("="*70)
        print(f"Mean Energy: {self.baseline_data['mean_energy']:.2f} ± {self.baseline_data['std_energy']:.2f} J")
        print(f"Mean Runtime: {self.baseline_data['mean_runtime']:.2f} ± {self.baseline_data['std_runtime']:.2f} s")
        print(f"Energy CV: {self.baseline_data['std_energy']/self.baseline_data['mean_energy']*100:.2f}%")
        
        return self.baseline_data
    
    def run_optimized_test(self, num_runs: int = 20) -> Dict:
        """
        Run tests with communication phase optimization enabled
        """
        print("\n" + "="*70)
        print("RUNNING OPTIMIZED TESTS (WITH COMM PHASE OPTIMIZATION)")
        print("="*70)
        
        energy_samples = []
        runtime_samples = []
        energy_saved_samples = []
        
        for run in range(num_runs):
            print(f"\n[Optimized Run {run+1}/{num_runs}]")
            
            # Start monitor in background
            monitor_proc = subprocess.Popen(
                ['sudo', 'python3', 'comm_phase_monitor.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for monitor initialization
            time.sleep(3)
            
            # Run miniMD
            start_time = time.time()
            
            cmd = [
                'mpirun', '--oversubscribe', '-np', '8',
                './miniMD_openmpi', '-i', 'in.lj.miniMD', '-n', '2000'
            ]
            
            perf_cmd = [
                'perf', 'stat', '-a',
                '-e', 'power/energy-pkg/',
                '-e', 'power/energy-ram/',
                '--', *cmd
            ]
            
            result = subprocess.run(
                perf_cmd,
                capture_output=True,
                text=True,
                cwd='miniMD/ref'
            )
            
            runtime = time.time() - start_time
            
            # Stop monitor
            monitor_proc.terminate()
            monitor_proc.wait()
            
            # Parse energy
            energy_pkg = self._parse_perf_energy(result.stderr, 'energy-pkg')
            energy_ram = self._parse_perf_energy(result.stderr, 'energy-ram')
            total_energy = energy_pkg + energy_ram
            
            energy_samples.append(total_energy)
            runtime_samples.append(runtime)
            
            # Calculate savings
            if self.baseline_data:
                energy_saved_pct = (1 - total_energy / self.baseline_data['mean_energy']) * 100
                energy_saved_samples.append(energy_saved_pct)
            
            print(f"  Energy: {total_energy:.2f} J, Runtime: {runtime:.2f} s")
            if energy_saved_samples:
                print(f"  Energy Saved: {energy_saved_samples[-1]:.2f}%")
            
            time.sleep(2)
        
        self.optimized_data = {
            'energy': energy_samples,
            'runtime': runtime_samples,
            'energy_saved': energy_saved_samples,
            'mean_energy': statistics.mean(energy_samples),
            'std_energy': statistics.stdev(energy_samples),
            'mean_runtime': statistics.mean(runtime_samples),
            'std_runtime': statistics.stdev(runtime_samples),
            'mean_energy_saved': statistics.mean(energy_saved_samples) if energy_saved_samples else 0,
            'std_energy_saved': statistics.stdev(energy_saved_samples) if len(energy_saved_samples) > 1 else 0
        }
        
        print("\n" + "="*70)
        print("OPTIMIZED RESULTS")
        print("="*70)
        print(f"Mean Energy: {self.optimized_data['mean_energy']:.2f} ± {self.optimized_data['std_energy']:.2f} J")
        print(f"Mean Runtime: {self.optimized_data['mean_runtime']:.2f} ± {self.optimized_data['std_runtime']:.2f} s")
        print(f"Mean Energy Saved: {self.optimized_data['mean_energy_saved']:.2f} ± {self.optimized_data['std_energy_saved']:.2f}%")
        
        return self.optimized_data
    
    def validate_against_blueprint(self) -> Dict:
        """
        Validate results against blueprint specifications
        Blueprint requirements:
        - 10-15% energy reduction during comm phases
        - <3% performance impact
        - p < 0.05 statistical significance
        """
        print("\n" + "="*70)
        print("BLUEPRINT VALIDATION")
        print("="*70)
        
        validation_results = {
            'meets_energy_target': False,
            'meets_performance_target': False,
            'statistically_significant': False,
            'overall_pass': False
        }
        
        if not self.baseline_data or not self.optimized_data:
            print("ERROR: Missing baseline or optimized data")
            return validation_results
        
        # 1. Energy Savings Validation
        print("\n1. ENERGY SAVINGS VALIDATION")
        print("-" * 70)
        
        target_energy_reduction = 10.0  # 10% minimum from blueprint
        achieved_reduction = self.optimized_data['mean_energy_saved']
        
        print(f"Target Energy Reduction: ≥{target_energy_reduction}%")
        print(f"Achieved Energy Reduction: {achieved_reduction:.2f}%")
        
        if achieved_reduction >= target_energy_reduction:
            print("✓ PASS: Meets energy savings target")
            validation_results['meets_energy_target'] = True
        else:
            print("✗ FAIL: Does not meet energy savings target")
        
        # 2. Performance Impact Validation
        print("\n2. PERFORMANCE IMPACT VALIDATION")
        print("-" * 70)
        
        max_performance_impact = 3.0  # 3% from blueprint
        baseline_runtime = self.baseline_data['mean_runtime']
        optimized_runtime = self.optimized_data['mean_runtime']
        
        runtime_impact = (optimized_runtime - baseline_runtime) / baseline_runtime * 100
        
        print(f"Max Allowed Performance Impact: <{max_performance_impact}%")
        print(f"Actual Performance Impact: {runtime_impact:+.2f}%")
        
        if abs(runtime_impact) < max_performance_impact:
            print("✓ PASS: Performance impact within acceptable range")
            validation_results['meets_performance_target'] = True
        else:
            print("✗ FAIL: Performance impact exceeds acceptable range")
        
        # 3. Statistical Significance Testing
        print("\n3. STATISTICAL SIGNIFICANCE TESTING")
        print("-" * 70)
        
        # Two-sample t-test for energy difference
        t_stat, p_value = stats.ttest_ind(
            self.baseline_data['energy'],
            self.optimized_data['energy'],
            alternative='greater'  # baseline > optimized
        )
        
        print(f"Two-sample t-test (energy):")
        print(f"  t-statistic: {t_stat:.4f}")
        print(f"  p-value: {p_value:.6f}")
        
        if p_value < 0.05:
            print("✓ PASS: Energy reduction is statistically significant (p < 0.05)")
            validation_results['statistically_significant'] = True
        else:
            print("✗ FAIL: Energy reduction is not statistically significant")
        
        # Effect size (Cohen's d)
        pooled_std = np.sqrt((self.baseline_data['std_energy']**2 + 
                             self.optimized_data['std_energy']**2) / 2)
        cohens_d = (self.baseline_data['mean_energy'] - 
                   self.optimized_data['mean_energy']) / pooled_std
        
        print(f"  Effect size (Cohen's d): {cohens_d:.4f}")
        if cohens_d > 0.5:
            print("  Effect size: Medium to Large")
        elif cohens_d > 0.2:
            print("  Effect size: Small to Medium")
        else:
            print("  Effect size: Small")
        
        # 4. System Noise Threshold
        print("\n4. NOISE THRESHOLD VALIDATION")
        print("-" * 70)
        
        noise_threshold = 4.0  # 4% from blueprint (The Case of Missing Supercomputer Performance)
        
        print(f"System Noise Threshold: {noise_threshold}%")
        print(f"Achieved Savings: {achieved_reduction:.2f}%")
        
        if achieved_reduction > noise_threshold:
            print(f"✓ PASS: Savings exceed noise threshold by {achieved_reduction - noise_threshold:.2f}%")
        else:
            print(f"✗ WARNING: Savings may be within system noise margin")
        
        # Overall validation
        validation_results['overall_pass'] = (
            validation_results['meets_energy_target'] and
            validation_results['meets_performance_target'] and
            validation_results['statistically_significant']
        )
        
        print("\n" + "="*70)
        print("OVERALL VALIDATION RESULT")
        print("="*70)
        
        if validation_results['overall_pass']:
            print("✓✓✓ PASS: All blueprint requirements met ✓✓✓")
        else:
            print("✗✗✗ FAIL: Some blueprint requirements not met ✗✗✗")
        
        return validation_results
    
    def _parse_perf_energy(self, perf_output: str, domain: str) -> float:
        """Parse energy value from perf stat output"""
        for line in perf_output.splitlines():
            if domain in line:
                # Format: "    123.45 Joules power/energy-pkg/"
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return float(parts[0].replace(',', ''))
                    except ValueError:
                        continue
        return 0.0
    
    def generate_report(self, output_file: str = "validation_report.json"):
        """Generate comprehensive validation report"""
        report = {
            'baseline': self.baseline_data,
            'optimized': self.optimized_data,
            'validation': self.validate_against_blueprint(),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n[REPORT] Validation report saved to {output_file}")
        return report

# =====================================================
#           COMMUNICATION PATTERN VALIDATOR
# =====================================================

class CommPatternValidator:
    """Validates that communication phases are correctly detected"""
    
    def __init__(self, monitor_csv: str = "comm_phase_monitor.csv"):
        self.csv_path = monitor_csv
        
    def validate_phase_detection(self) -> Dict:
        """
        Validate phase detection accuracy
        Blueprint requirement: 95% accuracy
        """
        import pandas as pd
        
        print("\n" + "="*70)
        print("PHASE DETECTION VALIDATION")
        print("="*70)
        
        df = pd.read_csv(self.csv_path)
        
        # Analyze phase transitions
        phase_changes = (df['phase'] != df['phase'].shift()).sum()
        total_samples = len(df)
        
        print(f"\nTotal samples: {total_samples}")
        print(f"Phase transitions: {phase_changes}")
        print(f"Average phase duration: {total_samples / phase_changes:.1f} samples")
        
        # Validate detection criteria
        comm_phases = df[df['phase'] == 'COMMUNICATION']
        compute_phases = df[df['phase'] == 'COMPUTE']
        
        print(f"\nCommunication phases: {len(comm_phases)} ({len(comm_phases)/total_samples*100:.1f}%)")
        print(f"Compute phases: {len(compute_phases)} ({len(compute_phases)/total_samples*100:.1f}%)")
        
        # Check IPC separation (key indicator of correct detection)
        comm_ipc_mean = comm_phases['ipc'].mean()
        compute_ipc_mean = compute_phases['ipc'].mean()
        ipc_separation = abs(compute_ipc_mean - comm_ipc_mean)
        
        print(f"\nIPC Analysis:")
        print(f"  Communication phase IPC: {comm_ipc_mean:.3f}")
        print(f"  Compute phase IPC: {compute_ipc_mean:.3f}")
        print(f"  Separation: {ipc_separation:.3f}")
        
        # Good separation indicates accurate detection
        if ipc_separation > 0.3:
            print("  ✓ Strong phase separation detected")
            detection_quality = "GOOD"
        elif ipc_separation > 0.15:
            print("  ~ Moderate phase separation detected")
            detection_quality = "MODERATE"
        else:
            print("  ✗ Weak phase separation detected")
            detection_quality = "POOR"
        
        # Check wait ratio in comm phases
        comm_wait_mean = comm_phases['wait_ratio'].mean()
        print(f"\nCommunication Wait Ratio: {comm_wait_mean:.3f}")
        if comm_wait_mean > 0.6:
            print("  ✓ High wait ratio confirms communication detection")
        
        return {
            'total_samples': total_samples,
            'phase_changes': phase_changes,
            'ipc_separation': ipc_separation,
            'detection_quality': detection_quality,
            'comm_phase_percentage': len(comm_phases) / total_samples * 100
        }

# =====================================================
#                INTEGRATION TESTER
# =====================================================

class IntegrationTester:
    """Tests full system integration"""
    
    def __init__(self):
        self.validator = CommunicationPhaseValidator()
        self.pattern_validator = CommPatternValidator()
        
    def run_full_test_suite(self):
        """Execute complete testing protocol"""
        print("="*70)
        print("COMMUNICATION PHASE OPTIMIZATION - FULL TEST SUITE")
        print("="*70)
        print("\nThis will take approximately 40-60 minutes for 20 runs each phase")
        print("(20 baseline + 20 optimized runs with 2s cooldown)")
        
        input("\nPress Enter to begin testing...")
        
        # Step 1: Baseline
        print("\n" + "="*70)
        print("STEP 1: BASELINE TESTING")
        print("="*70)
        baseline_data = self.validator.run_baseline_test(num_runs=20)
        
        # Step 2: Optimized
        print("\n" + "="*70)
        print("STEP 2: OPTIMIZED TESTING")
        print("="*70)
        optimized_data = self.validator.run_optimized_test(num_runs=20)
        
        # Step 3: Validation
        print("\n" + "="*70)
        print("STEP 3: BLUEPRINT VALIDATION")
        print("="*70)
        validation_results = self.validator.validate_against_blueprint()
        
        # Step 4: Phase Detection Validation
        print("\n" + "="*70)
        print("STEP 4: PHASE DETECTION VALIDATION")
        print("="*70)
        detection_results = self.pattern_validator.validate_phase_detection()
        
        # Step 5: Generate Report
        report = self.validator.generate_report()
        
        # Final Summary
        self._print_final_summary(validation_results, detection_results)
        
        return report
    
    def _print_final_summary(self, validation: Dict, detection: Dict):
        """Print final test summary"""
        print("\n" + "="*70)
        print("FINAL TEST SUMMARY")
        print("="*70)
        
        print("\nBlueprint Compliance:")
        print(f"  Energy Target: {'✓ PASS' if validation['meets_energy_target'] else '✗ FAIL'}")
        print(f"  Performance Target: {'✓ PASS' if validation['meets_performance_target'] else '✗ FAIL'}")
        print(f"  Statistical Significance: {'✓ PASS' if validation['statistically_significant'] else '✗ FAIL'}")
        
        print("\nPhase Detection:")
        print(f"  Quality: {detection['detection_quality']}")
        print(f"  IPC Separation: {detection['ipc_separation']:.3f}")
        
        print("\nOverall Result:")
        if validation['overall_pass']:
            print("  ✓✓✓ SYSTEM VALIDATED - READY FOR DEPLOYMENT ✓✓✓")
        else:
            print("  ✗✗✗ SYSTEM REQUIRES TUNING ✗✗✗")
        
        print("\n" + "="*70)

# =====================================================
#                     MAIN ENTRY
# =====================================================

def main():
    """Main entry point for testing framework"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Communication Phase Optimization Testing')
    parser.add_argument('--mode', choices=['baseline', 'optimized', 'validate', 'full'],
                       default='full', help='Test mode to run')
    parser.add_argument('--runs', type=int, default=20, 
                       help='Number of test runs (default: 20)')
    
    args = parser.parse_args()
    
    if args.mode == 'full':
        tester = IntegrationTester()
        tester.run_full_test_suite()
    elif args.mode == 'baseline':
        validator = CommunicationPhaseValidator()
        validator.run_baseline_test(num_runs=args.runs)
    elif args.mode == 'optimized':
        validator = CommunicationPhaseValidator()
        validator.run_optimized_test(num_runs=args.runs)
    elif args.mode == 'validate':
        validator = CommunicationPhaseValidator()
        validator.validate_against_blueprint()

if __name__ == "__main__":
    main()
