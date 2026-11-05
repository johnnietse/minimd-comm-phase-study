import pandas as pd
import numpy as np
import glob
import os

# ================= CONFIGURATION =================
TRUTH_FILE_PATTERN = "truth_rank_*.csv"
MONITOR_FILE = "monitor_new.csv"
# =================================================

def parse_all_ranks(file_pattern):
    """
    Reads ALL rank logs and creates a 'Vote' for every timestamp.
    """
    all_files = glob.glob(file_pattern)
    print(f"Loading ground truth from {len(all_files)} ranks...")
    
    events = []
    
    for f in all_files:
        try:
            df = pd.read_csv(f)
            df.columns = df.columns.str.strip()
            
            # Convert START/END events into intervals
            # We assume the file is sorted by time
            start_time = None
            curr_phase = None
            
            for _, row in df.iterrows():
                t = row['Timestamp']
                evt = row['Event'].strip()
                ph = row['Phase'].strip()
                
                if evt == "START":
                    start_time = t
                    curr_phase = ph
                elif evt == "END" and ph == curr_phase and start_time is not None:
                    events.append((start_time, t, ph))
                    start_time = None
        except Exception as e:
            print(f"Skipping broken file {f}: {e}")

    # Create a DataFrame of ALL intervals from ALL ranks
    print(f"Processing {len(events)} total phase events...")
    return pd.DataFrame(events, columns=['start', 'end', 'phase'])

def get_system_consensus(truth_df, win_start, win_end):
    """
    Calculates what % of the ENTIRE SYSTEM (all ranks) was in what phase
    during the window.
    """
    # Filter for intervals that overlap the window
    # Overlap logic: (Start < WinEnd) AND (End > WinStart)
    overlaps = truth_df[
        (truth_df['start'] < win_end) & 
        (truth_df['end'] > win_start)
    ].copy()
    
    if overlaps.empty:
        return None

    # Clip intervals to the window size
    overlaps['clip_start'] = overlaps['start'].clip(lower=win_start)
    overlaps['clip_end'] = overlaps['end'].clip(upper=win_end)
    overlaps['duration'] = overlaps['clip_end'] - overlaps['clip_start']
    
    # Sum durations by phase
    phase_counts = overlaps.groupby('phase')['duration'].sum()
    total_duration = phase_counts.sum()
    
    if total_duration == 0:
        return None

    # Calculate percentages
    stats = (phase_counts / total_duration * 100).sort_values(ascending=False)
    return stats

def validate_consensus(monitor_csv, truth_df):
    mon_df = pd.read_csv(monitor_csv)
    mon_df['prev_timestamp'] = mon_df['timestamp'].shift(1)
    
    results = []
    
    print(f"Validating {len(mon_df)} samples against system consensus (STRICT MODE)...")
    
    for i, row in mon_df.iterrows():
        if pd.isna(row['prev_timestamp']): continue
        
        t_start = row['prev_timestamp']
        t_end = row['timestamp']
        pred = row['phase']
        
        consensus = get_system_consensus(truth_df, t_start, t_end)
        
        if consensus is None: continue
        
        # --- STRICT LOGIC ---
        # The Winner takes all.
        primary_actual = consensus.index[0]
        primary_pct = consensus.iloc[0]
        
        is_correct = (pred == primary_actual)

        results.append({
            'timestamp': t_end,
            'predicted': pred,
            'actual_primary': primary_actual,
            'actual_pct': primary_pct,
            'correct': is_correct
        })
        
    return pd.DataFrame(results)

if __name__ == "__main__":
    # 1. Load All Ranks
    truth_df = parse_all_ranks(TRUTH_FILE_PATTERN)
    
    # 2. Validate
    if not truth_df.empty:
        res = validate_consensus(MONITOR_FILE, truth_df)
        
        # 3. Report
        acc = res['correct'].mean() * 100
        print(f"\nFINAL SYSTEM-WIDE ACCURACY: {acc:.2f}%")
        print(pd.crosstab(res['actual_primary'], res['predicted']))
        print("\nSample:")
        print(res.head(10))
