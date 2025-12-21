import pandas as pd
import numpy as np
import sys, os

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio

BUCKET_NAME = "mlbb-lakehouse"

def create_ml_features(df_silver_enriched):
    """
    Mengubah data Silver menjadi Fitur ML yang LEBIH KAYA.
    FIXED: Menangani tipe data campuran agar Parquet tidak error.
    """
    print("--LOGIC: Creating Rich ML Features (Pivot + Stats)")
    
    # Filter hanya fase PICK
    df_picks = df_silver_enriched[df_silver_enriched['phase'] == 'pick'].copy()
    
    # Pastikan kolom numerik aman (isi NaN dengan 0) sebelum pivot
    cols_to_fix = ['win_rate', 'counter_score', 'tier_score']
    for col in cols_to_fix:
        if col in df_picks.columns:
            df_picks[col] = pd.to_numeric(df_picks[col], errors='coerce').fillna(0)
    
    matches = []
    
    # Group by Match ID
    for match_id, group in df_picks.groupby('match_id'):
        row_data = {'match_id': match_id}
        
        # --- 1. PROSES TIM KIRI (T1) ---
        t1_group = group[group['team_side'] == 'left'].sort_values('order')
        if len(t1_group) > 0:
            row_data['Label_Winner'] = 1 if t1_group['is_winner_team'].iloc[0] else 0
            row_data['T1_Avg_WinRate'] = t1_group['win_rate'].mean()
            row_data['T1_Total_Counter'] = t1_group['counter_score'].sum()
            row_data['T1_Avg_Tier'] = t1_group['tier_score'].mean()
            
            heroes = t1_group['hero_name_normalized'].values
            for i, hero in enumerate(heroes):
                if i < 5: 
                    row_data[f'T1_Hero_{i+1}'] = str(hero) # Pastikan String

        # --- 2. PROSES TIM KANAN (T2) ---
        t2_group = group[group['team_side'] == 'right'].sort_values('order')
        if len(t2_group) > 0:
            row_data['T2_Avg_WinRate'] = t2_group['win_rate'].mean()
            row_data['T2_Total_Counter'] = t2_group['counter_score'].sum()
            row_data['T2_Avg_Tier'] = t2_group['tier_score'].mean()
            
            heroes = t2_group['hero_name_normalized'].values
            for i, hero in enumerate(heroes):
                if i < 5:
                    row_data[f'T2_Hero_{i+1}'] = str(hero) # Pastikan String
        
        matches.append(row_data)
        
    df_features = pd.DataFrame(matches)
    
    # --- PERBAIKAN DI SINI ---
    
    # 1. Pisahkan kolom Hero dan Statistik
    hero_cols = [c for c in df_features.columns if 'Hero_' in c]
    stats_cols = [c for c in df_features.columns if 'Hero_' not in c]
    
    # 2. Isi NaN Khusus Kolom Hero dengan "Unknown" (String)
    if len(hero_cols) > 0:
        df_features[hero_cols] = df_features[hero_cols].fillna("Unknown")
        # Paksa jadi string biar parquet gak rewel
        for c in hero_cols:
            df_features[c] = df_features[c].astype(str)
            
    # 3. Isi NaN Khusus Kolom Statistik dengan 0 (Angka)
    if len(stats_cols) > 0:
        df_features[stats_cols] = df_features[stats_cols].fillna(0)

    return df_features

def create_hero_leaderboard(df_silver_enriched):
    """ Sama seperti sebelumnya """
    print("--LOGIC: Creating Hero Leaderboard")
    df = df_silver_enriched[df_silver_enriched['phase'] == 'pick']
    leaderboard = df.groupby('hero_name_normalized').agg(
        total_picks=('match_id', 'count'),
        total_wins=('is_winner_team', 'sum'),
        avg_counter_score=('counter_score', 'mean')
    ).reset_index()
    leaderboard['win_rate'] = leaderboard['total_wins'] / leaderboard['total_picks']
    return leaderboard

def run_gold_pipeline():
    print('--- PIPELINE TRANSFORMING GOLD LAYER ---')
    
    # Load Silver
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    if df_silver is None:
        print("Silver data not found!")
        return

    # Create ML Data
    df_ml = create_ml_features(df_silver)
    upload_df_to_minio(df_ml, BUCKET_NAME, "gold/features_draft_model.parquet", file_format='parquet')
    print(f"DONE: ML Features saved ({len(df_ml)} matches)")

    # Create Dashboard Data
    df_dashboard = create_hero_leaderboard(df_silver)
    upload_df_to_minio(df_dashboard, BUCKET_NAME, "gold/hero_leaderboard.parquet", file_format='parquet')
    print(f"DONE: Leaderboard saved ({len(df_dashboard)} heroes)")

if __name__ == "__main__":
    run_gold_pipeline()