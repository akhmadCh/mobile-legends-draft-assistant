import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import os

# Menambahkan root project ke path agar bisa import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "mlbb-lakehouse"

# --- 1. HELPER FUNCTIONS (Fungsi Bantu Sederhana) ---

def get_timestamp():
    """Mengembalikan waktu saat ini untuk metadata."""
    return datetime.now()

def normalize_name(text):
    """
    Membersihkan nama hero.
    Contoh: "Claude 2024" -> "claude"
    Logic: Lowercase -> Hapus Tahun (4 digit) -> Hapus spasi
    """
    if not isinstance(text, str):
        return None
    
    # Lowercase dan strip spasi
    clean = text.lower().strip()
    
    # Hapus tahun (angka 4 digit) jika ada, misal ' 2024'
    # Menggunakan regex sederhana (\d{4})
    clean = re.sub(r'\s?\d{4}', '', clean)
    
    return clean

def clean_percentage(val):
    """Ubah '53.4%' (str) jadi 53.4 (float)."""
    if isinstance(val, str):
        return float(val.replace('%', '').strip())
    return val

def parse_string_to_list(text_str):
    """
    Ubah string CSV "Akai, Tigreal" menjadi List ['Akai', 'Tigreal'].
    """
    if pd.isna(text_str) or text_str == "":
        return []
    # Split koma, lalu bersihkan spasi di tiap item
    return [x.strip() for x in text_str.split(',') if x.strip()]

def get_tier_score(tier):
    """Konversi Tier huruf ke skor angka."""
    scores = {
        'SS': 5,
        'S': 4,
        'A': 3,
        'B': 2,
        'C': 1,
        'D': 0
    }
    # Kembalikan 0 jika tier tidak dikenali
    return scores.get(tier, 0)

# --- 2. MAIN PROCESSING FUNCTIONS ---

def process_stats_sql():
    print("\n[1/4] Memproses Hero Stats (SQL Source)...")
    
    # Baca data raw (CSV rasa SQL)
    df = read_df_from_minio(BUCKET_NAME, "raw/internal_db/hero_master_sql.csv")
    
    if df is not None:
        # Standardisasi nama kolom (snake_case)
        df.columns = ['hero_name_raw', 'win_rate', 'pick_rate', 'ban_rate', 'speciality']
        
        # Buat kolom normalized
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_name)
        
        # Bersihkan persentase
        for col in ['win_rate', 'pick_rate', 'ban_rate']:
            df[col] = df[col].apply(clean_percentage)
            
        # Tambah Metadata Lineage
        df['data_source'] = 'internal_sql'
        df['ingested_at'] = get_timestamp()
        
        # Simpan Parquet
        upload_df_to_minio(df, BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
        print("✅ Berhasil simpan: bronze_hero_stats.parquet")

def process_meta_tier():
    print("\n[2/4] Memproses Meta Tier...")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/hero_meta/meta_tier_raw.csv")
    
    if df is not None:
        # Rename kolom manual agar aman
        df = df.rename(columns={
            'Nama Hero': 'hero_name_raw',
            'Tier': 'tier_raw',
            'Score': 'score',
            'Image URL': 'image_url',
            'Hero ID': 'hero_id' # Keep ID asli
        })
        
        # Kolom normalized
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_name)
        
        # Tier Scoring
        df['tier_score'] = df['tier_raw'].apply(get_tier_score)
        
        # Metadata
        df['data_source'] = 'web_scraping_meta'
        df['ingested_at'] = get_timestamp()
        
        # Simpan (Hanya ambil kolom penting)
        cols_to_save = ['hero_id', 'hero_name_raw', 'hero_name_normalized', 'tier_raw', 'tier_score', 'score', 'image_url', 'data_source', 'ingested_at']
        upload_df_to_minio(df[cols_to_save], BUCKET_NAME, "bronze/meta/bronze_hero_meta.parquet", file_format='parquet')
        print("✅ Berhasil simpan: bronze_hero_meta.parquet")

def process_counter():
    print("\n[3/4] Memproses Hero Counter...")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/counter/data_counter.csv")
    
    if df is not None:
        # Rename kolom ke snake_case
        df.columns = df.columns.str.lower()
        df = df.rename(columns={
            'target_name': 'hero_name_raw',
            'counter_name': 'counter_name_raw'
        })
        
        # Normalisasi (Untuk kedua kolom nama)
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_name)
        df['counter_name_normalized'] = df['counter_name_raw'].apply(normalize_name)
        
        # Tier Score untuk Counter
        if 'tier' in df.columns:
            df['counter_tier_score'] = df['tier'].apply(get_tier_score)
            
        # Metadata
        df['data_source'] = 'web_scraping_counter'
        df['ingested_at'] = get_timestamp()
        
        upload_df_to_minio(df, BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')
        print("✅ Berhasil simpan: bronze_hero_counter.parquet")

def process_mpl_matches():
    print("\n[4/4] Memproses MPL Matches (ID & PH)...")
    
    # Baca kedua file
    df_id = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_id_s16.csv")
    df_ph = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_ph_s16.csv")
    df_my = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_my_s16.csv")
    
    # Cek jika data ada
    if df_id is not None and df_ph is not None and df_my is not None:
        
        # --- A. Tambah Metadata Source (Penting untuk Traceability) ---
        df_id['region'] = 'ID'
        df_id['tournament'] = 'MPL ID S16'
        df_id['source_file'] = 'mpl_id_s16.csv'
        
        df_ph['region'] = 'PH'
        df_ph['tournament'] = 'MPL PH S16'
        df_ph['source_file'] = 'mpl_ph_s16.csv'
        
        df_ph['region'] = 'MY'
        df_ph['tournament'] = 'MPL MY S16'
        df_ph['source_file'] = 'mpl_my_s16.csv'
        
        # --- B. Union Data ---
        df_combined = pd.concat([df_id, df_ph, df_my], ignore_index=True)
        
        # Rename kolom jadi lowercase semua
        df_combined.columns = df_combined.columns.str.lower()
        
        # --- C. Parsing List & Normalisasi ---
        # Kolom target: ban dan pick
        target_cols = ['left_bans', 'left_picks', 'right_bans', 'right_picks']
        
        for col in target_cols:
            if col in df_combined.columns:
                # 1. Buat kolom RAW (Format List/Array)
                # Parse string "HeroA, HeroB" -> ["HeroA", "HeroB"]
                col_raw_name = f"{col}_raw"
                df_combined[col_raw_name] = df_combined[col].apply(parse_string_to_list)
                
                # 2. Buat kolom NORMALIZED (Format List/Array)
                # ["Claude 2024", "Hylos"] -> ["claude", "hylos"]
                # Menggunakan list comprehension di dalam lambda
                col_norm_name = f"{col}_normalized"
                df_combined[col_norm_name] = df_combined[col_raw_name].apply(
                    lambda x: [normalize_name(hero) for hero in x]
                )
        
        # Metadata Lineage
        df_combined['ingested_at'] = get_timestamp()
        
        # Hapus kolom string lama agar tidak duplikat (opsional, tapi lebih rapi)
        df_combined = df_combined.drop(columns=target_cols)
        
        # Simpan Parquet
        upload_df_to_minio(df_combined, BUCKET_NAME, "bronze/tournament_matches/bronze_mpl_matches.parquet", file_format='parquet')
        print(f"✅ Berhasil simpan: bronze_mpl_matches.parquet ({len(df_combined)} rows)")

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    print("🚀 Memulai Pipeline Bronze Layer...")
    
    try:
        process_stats_sql()
        process_meta_tier()
        process_counter()
        process_mpl_matches()
        print("\n🏁 Pipeline Selesai. Cek MinIO folder 'bronze/'")
    except Exception as e:
        print(f"\n❌ Terjadi Error Kritis: {e}")