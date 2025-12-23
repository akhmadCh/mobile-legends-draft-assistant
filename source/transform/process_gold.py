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

def create_hero_leaderboard(df_silver_enriched, df_master_hero):
    """
    Membuat Leaderboard yang mencakup SELURUH hero (bahkan yang 0 pick).
    Teknik: Left Join antara Master Hero (Kiri) dengan Statistik Match (Kanan).
    """
    print("--LOGIC: Creating Full Hero Leaderboard (Master + Stats)")
    
    # 1. Hitung Statistik dari Match (Hanya untuk hero yang laku)
    # Kita ambil data fase 'pick' saja untuk statistik performa
    df_picks = df_silver_enriched[df_silver_enriched['phase'] == 'pick']
    
    stats_agg = df_picks.groupby('hero_name_normalized').agg(
        total_picks=('match_id', 'count'),
        total_wins=('is_winner_team', 'sum'),
        avg_counter_score=('counter_score', 'mean'),
        # Statistik rata-rata dari Silver (yang sudah ada win_rate, ban_rate bawaan)
        win_rate_avg=('win_rate', 'mean'),
        ban_rate_avg=('ban_rate', 'mean'),
        pick_rate_avg=('pick_rate', 'mean')
    ).reset_index()

    # 2. Siapkan Master Data (Daftar Absen Hero)
    # Pastikan kita punya kolom kunci yang sama: 'hero_name_normalized'
    # Jika df_master belum punya, kita buatkan dulu normalisasinya
    if 'hero_name_normalized' not in df_master_hero.columns:
        # Normalisasi sederhana: lowercase + alphanumeric only
        df_master_hero['hero_name_normalized'] = df_master_hero['hero_name_raw'].astype(str).str.lower().str.replace(r'[^a-z0-9]', '', regex=True)

    # Ambil kolom-kolom penting dari master (Nama Asli, Role, Lane, dll)
    # Asumsi kolom di master: ['hero_name_raw', 'role', 'lane', 'hero_name_normalized']
    master_base = df_master_hero[['hero_name_raw', 'hero_name_normalized', 'role', 'lane']].drop_duplicates(subset=['hero_name_normalized'])

    # 3. THE MAGIC STEP: LEFT JOIN
    # Kiri: Master (Lengkap) | Kanan: Stats (Bolong-bolong)
    leaderboard = pd.merge(master_base, stats_agg, on='hero_name_normalized', how='left')
    
    # 4. Handling Data Kosong (Hero yang tidak pernah dipick/ban)
    # Hero seperti 'Aamon' akan punya stats NaN. Kita isi dengan 0.
    leaderboard['total_picks'] = leaderboard['total_picks'].fillna(0)
    leaderboard['total_wins'] = leaderboard['total_wins'].fillna(0)
    
    # Untuk rate, jika 0 match, maka statnya 0.0
    leaderboard['win_rate'] = leaderboard['win_rate_avg'].fillna(0.0)
    leaderboard['ban_rate'] = leaderboard['ban_rate_avg'].fillna(0.0)
    leaderboard['pick_rate'] = leaderboard['pick_rate_avg'].fillna(0.0)
    leaderboard['avg_counter_score'] = leaderboard['avg_counter_score'].fillna(0.0)

    # 5. Rapihkan
    leaderboard = leaderboard.drop(columns=['win_rate_avg', 'ban_rate_avg', 'pick_rate_avg'])
    leaderboard = leaderboard.sort_values(by=['total_picks', 'win_rate'], ascending=False).reset_index(drop=True)
    
    return leaderboard

def create_counter_matrix(df_counter_silver):
    """Fungsi helper untuk membuat tabel lookup counter (seperti diskusi sebelumnya)"""
    print("--LOGIC: Creating Counter Matrix Lookup")
    if df_counter_silver is None or df_counter_silver.empty:
        return pd.DataFrame()
        
    lookup = df_counter_silver[['hero_name_normalized', 'counter_name_normalized', 'score']].copy()
    lookup.rename(columns={
        'hero_name_normalized': 'Target_Name', 
        'counter_name_normalized': 'Counter_Name',
        'score': 'Score'
    }, inplace=True)
    return lookup

def run_gold_pipeline():
    print('--- PIPELINE TRANSFORMING GOLD LAYER ---')
    
    # load silver
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    df_master = read_df_from_minio(BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
    
    if df_silver is None:
        print("CRITICAL: Silver Data Match not found!")
        return
    if df_master is None:
        print("CRITICAL: Master Hero Data not found! Hero list will be incomplete.")
        # Fallback darurat: Gunakan unik hero dari silver (tetap akan hilang data Aamon)
        # Tapi sebaiknya pipeline berhenti disini
        return
    
    # 3. Create Leaderboard (FULL HERO)
    df_dashboard = create_hero_leaderboard(df_silver, df_master)
    upload_df_to_minio(df_dashboard, BUCKET_NAME, "gold/hero_leaderboard.parquet", file_format='parquet')
    print(f"DONE: Leaderboard saved ({len(df_dashboard)} heroes). Aamon harusnya ada sekarang.")

    # 4. Create Counter Matrix
    # (Opsional) Load counter data jika terpisah
    df_counter_bronze = read_df_from_minio(BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')
    df_lookup = create_counter_matrix(df_counter_bronze)
    if not df_lookup.empty:
        upload_df_to_minio(df_lookup, BUCKET_NAME, "gold/hero_counter_lookup.parquet", file_format='parquet')
        print("DONE: Counter Lookup saved.")

    # Create ML Data
    df_ml = create_ml_features(df_silver)
    upload_df_to_minio(df_ml, BUCKET_NAME, "gold/features_draft_model.parquet", file_format='parquet')
    print(f"DONE: ML Features saved ({len(df_ml)} matches)")
    
    # --- STEP 4: PREVIEW DATA ---
    print('\nPreview Data untuk verifikasi:')
    print('Sample 2 baris features tim:')
    print(df_ml.head(40))
    print('Sample 2 baris leaderboard hero:')
    print(df_dashboard.head(130))
    print('\nPreview Counter Lookup:')
    print(df_lookup.head(40))

if __name__ == "__main__":
    run_gold_pipeline()