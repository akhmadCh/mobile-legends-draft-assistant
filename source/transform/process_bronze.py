import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import os

# helper functions
from source.utils.helper_bronze import get_timestamp, normalize_hero_name, clean_percentage, get_tier_score, parse_string_to_list

# Menambahkan root project ke path agar bisa import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "mlbb-lakehouse"

def process_stats_sql():
    print("\n[1/4] Proses Hero Stats (SQL Source)")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/internal_db/hero_master_sql.csv")
    
    if df is not None:
        # standarisasi nama kolom
        df.columns = ['hero_name_raw', 'win_rate', 'pick_rate', 'ban_rate', 'speciality']
        
        # kolom normalized
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_hero_name)
        
        # cleaning tipe data persentase
        for col in ['win_rate', 'pick_rate', 'ban_rate']:
            df[col] = df[col].apply(clean_percentage)
            
        # add metadata
        df['data_source'] = 'internal_sql'
        df['ingested_at'] = get_timestamp()
        
        # save to parquet
        upload_df_to_minio(df, BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
        print(f"--DONE, bronze_hero_stats.parquet save to MinIO {BUCKET_NAME}")

def process_meta_tier():
    print("\n[2/4] Proses Meta Tier...")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/hero_meta/meta_tier_raw.csv")
    
    if df is not None:
        # rename columns
        df = df.rename(columns={
            'Nama Hero': 'hero_name_raw',
            'Tier': 'tier_raw',
            'Score': 'score',
            'Image URL': 'image_url',
            'Hero ID': 'hero_id' # Keep ID asli
        })
        
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_hero_name)
        
        # tier scoring
        df['tier_score'] = df['tier_raw'].apply(get_tier_score)
        
        # metadata
        df['data_source'] = 'web_scraping_meta'
        df['ingested_at'] = get_timestamp()
        
        cols_to_save = ['hero_id', 'hero_name_raw', 'hero_name_normalized', 'tier_raw', 'tier_score', 'score', 'image_url', 'data_source', 'ingested_at']
        upload_df_to_minio(df[cols_to_save], BUCKET_NAME, "bronze/meta/bronze_hero_meta.parquet", file_format='parquet')
        print(f"--DONE, bronze_hero_meta.parquet save to MinIO {BUCKET_NAME}")

def process_counter():
    print("\n[3/4] Proses Hero Counter...")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/counter/data_counter.csv")
    
    if df is not None:
        # rename columns
        df.columns = df.columns.str.lower()
        df = df.rename(columns={
            'target_name': 'hero_name_raw',
            'counter_name': 'counter_name_raw'
        })
        
        df['hero_name_normalized'] = df['hero_name_raw'].apply(normalize_hero_name)
        df['counter_name_normalized'] = df['counter_name_raw'].apply(normalize_hero_name)
        
        # tier score untuk counter
        if 'tier' in df.columns:
            df['counter_tier_score'] = df['tier'].apply(get_tier_score)
            
        # metadata
        df['data_source'] = 'web_scraping_counter'
        df['ingested_at'] = get_timestamp()
        
        upload_df_to_minio(df, BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')
        print(f"--DONE, bronze_hero_counter.parquet save to MinIO {BUCKET_NAME}")

def process_mpl_matches():
    print("\n[4/4] Proses MPL Matches (ID & PH)...")
    
    # read all tournament files
    df_id = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_id_s16.csv")
    df_ph = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_ph_s16.csv")
    df_my = read_df_from_minio(BUCKET_NAME, "raw/mpl_matches/mpl_my_s16.csv")
    
    # check the data
    if df_id is not None and df_ph is not None and df_my is not None:
        
        # add metadata (traceability)
        df_id['region'] = 'ID'
        df_id['tournament'] = 'MPL ID S16'
        df_id['source_file'] = 'mpl_id_s16.csv'
        
        df_ph['region'] = 'PH'
        df_ph['tournament'] = 'MPL PH S16'
        df_ph['source_file'] = 'mpl_ph_s16.csv'
        
        df_ph['region'] = 'MY'
        df_ph['tournament'] = 'MPL MY S16'
        df_ph['source_file'] = 'mpl_my_s16.csv'
        
        # union
        df_combined = pd.concat([df_id, df_ph, df_my], ignore_index=True)
        
        # rename
        df_combined.columns = df_combined.columns.str.lower()
        
        # parsing list and normalize
        # target columns: ban and pick
        target_cols = ['left_bans', 'left_picks', 'right_bans', 'right_picks']
        
        for col in target_cols:
            if col in df_combined.columns:
                # 1. raw columns
                # parsing string "HeroA, HeroB" -> ["HeroA", "HeroB"]
                col_raw_name = f"{col}_raw"
                df_combined[col_raw_name] = df_combined[col].apply(parse_string_to_list)
                
                # 2. normalized columns
                # ["Claude 2024", "Hylos"] -> ["claude", "hylos"]
                col_norm_name = f"{col}_normalized"
                df_combined[col_norm_name] = df_combined[col_raw_name].apply(
                    lambda x: [normalize_hero_name(hero) for hero in x]
                )
        
        # add metadata
        df_combined['ingested_at'] = get_timestamp()
        
        # drop the old columns
        df_combined = df_combined.drop(columns=target_cols)
        
        upload_df_to_minio(df_combined, BUCKET_NAME, "bronze/tournament_matches/bronze_mpl_matches.parquet", file_format='parquet')
        print(f"--DONE, bronze_mpl_matches.parquet save to MinIO {BUCKET_NAME}")

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    print("🚀 Memulai Pipeline Bronze Layer...")
    
    try:
        process_stats_sql()
        process_meta_tier()
        process_counter()
        process_mpl_matches()
        print("\nBRONZE PIPELINE SUCCESS. Cek 'bronze/' folder in MinIO")
    except Exception as e:
        print(f"\nERROR: {e}")