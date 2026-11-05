import pandas as pd
import numpy as np

# ---------------- CONFIGURATION ----------------
MONITOR_FILE = "test.csv"
TRUTH_FILE = "ground_truth.csv"
MONITOR_INTERVAL = 0.5  # We assume each monitor row represents this much time
# -----------------------------------------------

def validate():
    print(f"Loading {MONITOR_FILE} and {TRUTH_FILE}...")
    
    try:
        df_log = pd.read_csv(MONITOR_FILE)
        df_truth = pd.read_csv(TRUTH_FILE)
    except FileNotFoundError:
        print("[ERROR] Files not found.")
        return

    # --- 1. NORMALIZE COLUMNS & DATA ---
    if 'phase' in df_truth.columns:
        df_truth.rename(columns={'phase': 'actual_phase'}, inplace=True)

    # Universal Mapping: Collapse specific C++ tags to generic Monitor tags
    mapping = {
        'PARALLEL_COMPUTE': 'COMPUTE',
        'SERIAL_COMPUTE':   'COMPUTE',
        'MEMORY_BOUND':     'MEMORY_BOUND',
        'COMMUNICATION':    'COMMUNICATION',
        'IO_STORAGE':       'IO',
        'FINISHED':         'FINISHED'
    }

    df_truth['actual_phase'] = df_truth['actual_phase'].str.strip().replace(mapping)
    df_log['phase'] = df_log['phase'].str.strip().replace(mapping)

    # Convert Timestamps
    df_log['timestamp'] = pd.to_numeric(df_log['timestamp'])
    df_truth['timestamp'] = pd.to_numeric(df_truth['timestamp'])
    
    # Sort
    df_log = df_log.sort_values('timestamp')
    df_truth = df_truth.sort_values('timestamp')

    # --- 2. CALCULATE GROUND TRUTH DURATIONS ---
    # We need to know how long each ground truth phase lasted to do a weighted vote
    df_truth['next_ts'] = df_truth['timestamp'].shift(-1)
    # Fill the last timestamp with a small delta so it doesn't break
    df_truth['next_ts'] = df_truth['next_ts'].fillna(df_truth['timestamp'] + 0.001)
    df_truth['duration'] = df_truth['next_ts'] - df_truth['timestamp']

    # --- 3. MONITOR-FIRST VALIDATION LOOP ---
    print(f"Validating {len(df_log)} monitor samples...")
    
    results = []

    for i, row in df_log.iterrows():
        # Define the window for this monitor sample
        t_start = row['timestamp']
        # The window ends when the next sample begins, or t_start + interval
        if i < len(df_log) - 1:
            t_end = df_log.iloc[i+1]['timestamp']
        else:
            t_end = t_start + MONITOR_INTERVAL
        
        # Sanity check: If gap is huge (program paused), cap it
        if t_end - t_start > MONITOR_INTERVAL * 5:
            t_end = t_start + MONITOR_INTERVAL

        # EXTRACT GROUND TRUTH IN THIS WINDOW
        # We find all truth phases that overlap with [t_start, t_end]
        mask = (df_truth['next_ts'] > t_start) & (df_truth['timestamp'] < t_end)
        window_truth = df_truth[mask].copy()

        if window_truth.empty:
            # No ground truth data for this time (e.g., monitor started early)
            continue

        # CLIP DURATIONS TO WINDOW
        # If a phase started before t_start, we only count the part inside the window
        window_truth['effective_start'] = window_truth['timestamp'].clip(lower=t_start)
        window_truth['effective_end'] = window_truth['next_ts'].clip(upper=t_end)
        window_truth['effective_duration'] = window_truth['effective_end'] - window_truth['effective_start']

        # MAJORITY VOTE
        # Sum duration per phase
        vote = window_truth.groupby('actual_phase')['effective_duration'].sum()
        
        if vote.empty or vote.sum() == 0:
            continue
            
        # The winner is the phase that occupied the most time in this window
        majority_phase = vote.idxmax()
        confidence = vote.max() / vote.sum() # How dominant was this phase?

        # RECORD RESULT
        is_correct = (row['phase'] == majority_phase)
        results.append({
            'timestamp': t_start,
            'predicted': row['phase'],
            'actual': majority_phase,
            'confidence': confidence,
            'correct': is_correct,
            'ipc': row.get('ipc', 0),
            'power': row.get('pkg_power', 0)
        })

    # --- 4. CALCULATE METRICS ---
    df_res = pd.DataFrame(results)
    
    if df_res.empty:
        print("[ERROR] No overlapping time periods found between Monitor and Truth.")
        return

    accuracy = df_res['correct'].mean() * 100
    
    print("\n" + "="*40)
    print(f"  FINAL ACCURACY (Time-Weighted): {accuracy:.2f}%")
    print("="*40)
    
    print("\n--- CONFUSION MATRIX ---")
    print(pd.crosstab(df_res['actual'], df_res['predicted']))
    
    print("\n--- PER-PHASE ACCURACY ---")
    for phase in df_res['actual'].unique():
        subset = df_res[df_res['actual'] == phase]
        acc = subset['correct'].mean() * 100
        print(f"  {phase:15s}: {acc:.1f}% (n={len(subset)})")

    print("\n--- SAMPLE ERRORS ---")
    print(df_res[~df_res['correct']].head(10))

if __name__ == "__main__":
    validate()
