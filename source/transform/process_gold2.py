import pandas as pd
import numpy as np
import sys, os

# Menambahkan root project ke path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# helper functions
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "mlbb-lakehouse"

def transform_gold_pick_features(df_silver):
    """
    TABLE 1: GOLD DRAFT PICK FEATURES
    Fokus: Mengambil data fase PICK saja dan normalisasi fitur agar siap analisis.
    Output: Satu baris per hero yang di-pick.
    """
    print('--LOGIC: Filter Picks & Normalize Features')
    
    # 1. Ambil hanya fase 'pick' (Ban tidak dipakai untuk prediksi kemenangan dasar)
    df_picks = df_silver[df_silver['phase'] == 'pick'].copy()
    
    # 2. Normalisasi Counter Score
    # Agar range-nya rapi (misal -1 sampai 1) untuk masuk ke model nanti
    # Kita cari nilai absolut terbesar dulu
    max_abs_score = df_picks['counter_score'].abs().max()
    
    # hindari pembagian dengan nol jika data kosong
    if max_abs_score == 0: 
        max_abs_score = 1 
        
    df_picks['counter_score_norm'] = df_picks['counter_score'] / max_abs_score
    
    # 3. Pilih kolom-kolom yang "Daging" saja (Fitur Utama)
    selected_cols = [
        # Identitas
        'match_id', 'team_side', 'team_name', 'hero_name_normalized', 'is_winner_team',
        # Fitur Statistik Hero (Individual)
        'win_rate', 'pick_rate', 'tier_score', 
        # Fitur Interaksi (Lawan)
        'counter_score', 'counter_score_norm',
        # Fitur Komposisi
        'role', 'lane', 'speciality',
        # Audit
        'source_file'
    ]
    
    # pastikan kolom ada, kalau tidak ada di silver, kita skip errornya
    existing_cols = [c for c in selected_cols if c in df_picks.columns]
    
    df_gold_picks = df_picks[existing_cols].copy()
    df_gold_picks['processed_at'] = get_timestamp()
    
    return df_gold_picks

def transform_gold_match_features(df_gold_picks):
    """
    TABLE 2: GOLD MATCH FEATURES
    Fokus: Agregasi (Rata-rata/Sum) per Tim untuk input Machine Learning.
    Output: Satu baris per TIM per MATCH.
    """
    print('--LOGIC: Agregasi data per Tim (Siap Training ML)')
    
    # Kita akan "menggepengkan" data. Dari 5 baris hero -> 1 baris tim.
    # Naive Bayes suka input yang sederhana (rata-rata tim).
    
    agg_rules = {
        'is_winner_team': 'first',      # Label target (Menang/Kalah itu sama satu tim)
        'team_name': 'first',           # Nama tim
        'win_rate': 'mean',             # Rata-rata WR hero yg dipilih tim
        'tier_score': 'mean',           # Rata-rata kekuatan Meta
        'counter_score': 'mean',        # Rata-rata seberapa kuat meng-counter musuh
        'counter_score_norm': 'mean',   # Versi normalisasi
        'hero_name_normalized': 'count' # Hitung jumlah hero (buat validasi harus 5)
    }
    
    # Lakukan Group By
    df_match_features = df_gold_picks.groupby(['match_id', 'team_side']).agg(agg_rules).reset_index()
    
    # Rename kolom biar jelas maknanya
    df_match_features = df_match_features.rename(columns={
        'win_rate': 'avg_win_rate_team',
        'tier_score': 'avg_meta_score_team',
        'counter_score': 'avg_counter_score_team',
        'counter_score_norm': 'avg_counter_score_norm_team',
        'hero_name_normalized': 'total_heroes_count'
    })
    
    # Filter Data Bersih: Hapus match yang heronya kurang dari 5 (misal data error)
    # Model butuh input lengkap 5 hero
    df_clean = df_match_features[df_match_features['total_heroes_count'] == 5].copy()
    
    df_clean['processed_at'] = get_timestamp()
    
    # Report dikit berapa yang dibuang
    dropped_count = len(df_match_features) - len(df_clean)
    if dropped_count > 0:
        print(f"WARNING: Ada {dropped_count} tim yang datanya tidak lengkap (< 5 hero) dan dibuang.")
        
    return df_clean
 
def transform_gold_match_level(df_team):
    """
    GOLD MATCH LEVEL
    1 baris = 1 match
    Fitur = perbandingan left vs right
    """

    left = df_team[df_team['team_side'] == 'left']
    right = df_team[df_team['team_side'] == 'right']

    df_match = pd.merge(
        left,
        right,
        on='match_id',
        suffixes=('_left', '_right')
    )

    df_match['processed_at'] = get_timestamp()

    return df_match


def run_gold_pipeline():
    print('--- PIPELINE TRANSFORMING GOLD LAYER ---')
    print('Tujuan: Menyiapkan data final untuk Analytics & ML Training')
    
    # --- STEP 1: LOAD SILVER DATA ---
    print('\n1/4 Loading Silver Data...')
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    
    if df_silver is None:
        print("--ERROR: Data Silver tidak ditemukan. Jalankan process_silver.py dulu.")
        return

    # --- STEP 2: CREATE GOLD PICK FEATURES ---
    print('\n2/4 Create Table 1: Gold Draft Pick Features...')
    df_gold_picks = transform_gold_pick_features(df_silver)
    
    upload_df_to_minio(df_gold_picks, BUCKET_NAME, "gold/gold_draft_pick_features.parquet", file_format='parquet')
    print(f"DONE: gold_draft_pick_features.parquet ({len(df_gold_picks)} rows)")
    
    # --- STEP 3: CREATE GOLD MATCH FEATURES ---
    print('\n3/4 Create Table 2: Gold Match Features (ML Ready)...')
    df_gold_team = transform_gold_match_features(df_gold_picks)
    df_gold_matches = transform_gold_match_features(df_gold_picks)
    
    upload_df_to_minio(df_gold_matches, BUCKET_NAME, "gold/gold_match_features.parquet", file_format='parquet')
    print(f"DONE: gold_match_features.parquet ({len(df_gold_matches)} rows)")
    
    # --- STEP 4: PREVIEW DATA ---
    print('\n4/4 Preview Data untuk verifikasi:')
    print('Sample 2 baris features tim:')
    print(df_gold_matches[['team_name', 'avg_win_rate_team', 'avg_counter_score_team', 'is_winner_team']].head(2))
    
    print("\nGOLD PIPELINE COMPLETED")

if __name__ == "__main__":
    run_gold_pipeline()