import pandas as pd
import numpy as np
from scipy.optimize import differential_evolution
from collections import deque, Counter
import statistics

# ---------------- CONFIGURATION ----------------
# CORRECTED MAPPING: Distinct IDs for all phases to force real learning
PHASE_MAP = {
    "COMPUTE": 0,
    "COMMUNICATION": 1,
    "MEMORY_BOUND": 2, # CHANGED: Map to 2 (Distinguish from Compute)
    "IDLE": 3,
    "STORAGE": 4       # Added Storage
}
REVERSE_MAP = {v: k for k, v in PHASE_MAP.items()}

# ---------------- SIMULATION ENGINE ----------------
class FastPhaseSimulator:
    def __init__(self, data_df):
        self.data = data_df.to_dict('records')
        
    def run_with_params(self, params):
        # Unpack parameters
        W_COMP_PWR  = params[0]
        W_COMP_IPC  = params[1]
        W_COMP_UTIL = params[2]
        W_COMM_NET  = params[3]
        W_COMM_LOW_U= params[4]
        W_COMM_VAR  = params[5]
        W_MEM_MISS  = params[6]
        W_MEM_DRAM  = params[7]
        
        T_NET_MBPS  = params[8]
        T_EFF_UTIL  = params[9]
        T_SYNC_VAR  = params[10]
        
        history_power = deque(maxlen=20)
        history_ipc = deque(maxlen=20)
        history_miss = deque(maxlen=20)
        phase_seq = deque(maxlen=3)
        
        predictions = []
        
        # Initial Dynamic Thresholds
        dyn_pwr_comp = 180.0
        dyn_ipc_target = 1.6
        dyn_miss_target = 0.30
        
        for row in self.data:
            # Update History
            pwr = row['pkg_power']
            ipc = row['ipc']
            miss = row['miss_rate']
            
            history_power.append(pwr)
            history_ipc.append(ipc)
            history_miss.append(miss)
            
            if len(history_power) >= 20:
                dyn_pwr_comp = np.percentile(history_power, 75) * 0.90
                dyn_ipc_target = np.percentile(history_ipc, 75)
                dyn_miss_target = np.percentile(history_miss, 75)

            # Calculate Scores
            comp_score = 0.0
            comm_score = 0.0
            mem_score = 0.0
            
            # Compute Indicators
            if pwr > dyn_pwr_comp: comp_score += W_COMP_PWR
            if ipc > dyn_ipc_target: comp_score += W_COMP_IPC
            if row['cpu_util_eff'] > T_EFF_UTIL: comp_score += W_COMP_UTIL
            
            # Comm Indicators
            total_net = row['net_rx'] + row['net_tx']
            if total_net > T_NET_MBPS: comm_score += W_COMM_NET
            if row['cpu_util_eff'] < 40.0 and pwr > 50: comm_score += W_COMM_LOW_U
            if row['sync_var'] > T_SYNC_VAR: comm_score += W_COMM_VAR
            
            # Memory Indicators
            if ipc < 1.0 and miss > dyn_miss_target: mem_score += W_MEM_MISS
            if row['dram_power'] > 15.0: mem_score += W_MEM_DRAM # Updated to 15.0
            
            # --- PRIORITY DECISION LOGIC (Matches V18) ---
            
            # 1. STORAGE / IDLE (Gatekeeper)
            # High I/O wait (approx check) or Low Util+Low Power
            iowait_val = row.get('iowait', 0.0)
            
            if iowait_val > 5.0 or (row['cpu_util_eff'] < 30.0 and pwr < 100):
                 if pwr < 50.0:
                     raw_phase = PHASE_MAP["IDLE"]
                 else:
                     raw_phase = PHASE_MAP["STORAGE"]
            
            # 2. MEMORY (Dominates Compute if score is high)
            elif mem_score > 2.0:
                raw_phase = PHASE_MAP["MEMORY_BOUND"]
            
            # 3. COMM vs COMPUTE
            elif comm_score > comp_score:
                raw_phase = PHASE_MAP["COMMUNICATION"]
            else:
                raw_phase = PHASE_MAP["COMPUTE"]
                
            # Stabilization
            phase_seq.append(raw_phase)
            final_phase = raw_phase
            if len(phase_seq) >= 3:
                counts = Counter(list(phase_seq))
                most_common = counts.most_common(1)[0]
                if most_common[1] >= 2 and most_common[0] != raw_phase:
                    final_phase = most_common[0]
            
            predictions.append(final_phase)
            
        return predictions

# ---------------- OPTIMIZATION LOOP ----------------

def load_data():
    print("Loading data...")
    try:
        df_log = pd.read_csv("monitor_new.csv")
        df_truth = pd.read_csv("ground_truth.csv")
    except FileNotFoundError:
        print("[ERROR] Files not found! Check directory.")
        return pd.DataFrame()

    # 1. Force numeric and Sort
    df_log['timestamp'] = pd.to_numeric(df_log['timestamp'])
    df_truth['timestamp'] = pd.to_numeric(df_truth['timestamp'])
    df_log = df_log.sort_values('timestamp')
    df_truth = df_truth.sort_values('timestamp')

    # ---------------- MICRO-PHASE FILTERING (CRITICAL) ----------------
    # Filter out phases that are shorter than 0.1s (Nyquist limit for 0.2s sampler)
    # These create false negatives because the monitor literally cannot see them.
    df_truth['next_ts'] = df_truth['timestamp'].shift(-1)
    df_truth['duration'] = df_truth['next_ts'] - df_truth['timestamp']
    
    # Keep rows where duration > 0.1s (or is NaN, meaning the last row)
    pre_len = len(df_truth)
    mask_valid = (df_truth['duration'] > 0.05) | (df_truth['duration'].isna())
    df_truth = df_truth[mask_valid].copy()
    
    print(f"  Micro-phase Filter: Removed {pre_len - len(df_truth)} samples (< 100ms).")
    # ------------------------------------------------------------------

    # 2. ALIGNMENT
    # Look backward to find the state active at the sampling moment.
    # Shift -0.1s to center the 0.2s window.
    df_log['ts_aligned'] = df_log['timestamp'] - 0.1
    
    df_merged = pd.merge_asof(df_log, 
                              df_truth, 
                              left_on='ts_aligned',
                              right_on='timestamp', 
                              direction='backward',
                              suffixes=('_log', '_truth'))
    
    # 3. Cleanup
    df_merged = df_merged.dropna(subset=['actual_phase'])
    df_merged['truth_encoded'] = df_merged['actual_phase'].map(PHASE_MAP)
    
    # Drop rows where we couldn't map the phase (e.g., if log has weird string)
    df_merged = df_merged.dropna(subset=['truth_encoded'])
    
    print(f"Alignment: Successfully matched {len(df_merged)} samples.")
    print("\n--- SURVIVOR ANALYSIS ---")
    print(df_merged['actual_phase'].value_counts())
    print("-------------------------")
    return df_merged
    
    return df_merged

def objective_function(params, simulator, ground_truth):
    preds = simulator.run_with_params(params)
    correct = np.sum(np.array(preds) == ground_truth)
    accuracy = correct / len(ground_truth)
    return -accuracy 

def run_optimization():
    df = load_data()
    if df.empty or len(df) < 10:
        print("[ERROR] Not enough data after purification.")
        return

    simulator = FastPhaseSimulator(df)
    ground_truth = df['truth_encoded'].values
    
    bounds = [
        (0.5, 8.0), (0.5, 8.0), (0.5, 8.0), # Weights Comp (Expanded range)
        (0.5, 8.0), (0.5, 8.0), (0.5, 8.0), # Weights Comm
        (0.5, 8.0), (0.5, 8.0),             # Weights Mem
        (5.0, 100.0), # T_NET_MBPS 
        (50.0, 95.0), # T_EFF_UTIL 
        (2.0, 50.0),  # T_SYNC_VAR 
    ]
    
    print("Starting Optimization...")
    
    result = differential_evolution(
        objective_function, 
        bounds, 
        args=(simulator, ground_truth),
        strategy='best1bin',
        maxiter=50,       
        popsize=20,       
        polish=False,
        disp=True,
        workers=-1
    )
    
    print("\n---------------- RESULTS ----------------")
    acc = -result.fun * 100
    print(f"Best Accuracy Achieved: {acc:.2f}%")
    
    print("\nOptimal Parameters (Copy these to your IntelligentMonitorv17.py):")
    names = [
        "W_COMP_PWR", "W_COMP_IPC", "W_COMP_UTIL", 
        "W_COMM_NET", "W_COMM_LOW_U", "W_COMM_VAR", 
        "W_MEM_MISS", "W_MEM_DRAM", "T_NET_MBPS", 
        "T_EFF_UTIL", "T_SYNC_VAR"
    ]
    for name, val in zip(names, result.x):
        print(f"  {name}: {val:.4f}")

    if acc < 60.0:
        print("\n[NOTE] Accuracy is < 60%. Check if 'Memory' phases are actually distinguishable.")
        print("       If DRAM power/IPC looks identical to Compute, you may need to map MEMORY_BOUND -> 0.")

if __name__ == "__main__":
    run_optimization()
