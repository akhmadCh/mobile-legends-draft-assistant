import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp
from source.utils.helper_bronze import normalize_hero_name

BUCKET_NAME = "mlbb-lake"

def process_user_bronze():
    print("[User Pipeline] Processing Bronze...")
    # Baca RAW CSV
    df = read_df_from_minio(BUCKET_NAME, "raw/user_history/match_history_user.csv", file_format='csv')
    if df is None or df.empty:
        print("No raw user data found.")
        return None

    # Cleaning dasar
    df['ingested_at'] = get_timestamp()
    
    # Simpan sebagai Parquet (Bronze)
    upload_df_to_minio(df, BUCKET_NAME, "bronze/user_history/user_match_history.parquet", file_format='parquet')
    return df

def process_user_silver():
    print("[User Pipeline] Processing Silver...")
    # Baca Bronze
    df = read_df_from_minio(BUCKET_NAME, "bronze/user_history/user_match_history.parquet", file_format='parquet')
    if df is None: return None

    # Logic: Explode My Team untuk analisa per Hero
    # Format Raw "HeroA,HeroB" -> List -> Rows
    expanded_rows = []
    
    for _, row in df.iterrows():
        # Parsing string CSV manual atau pakai split
        my_heroes = str(row['my_team']).split(',')
        is_win = 1 if str(row['result']).strip().lower() == 'win' else 0
        
        for hero in my_heroes:
            hero_clean = hero.strip()
            if hero_clean:
                expanded_rows.append({
                    'timestamp': row['timestamp'],
                    'hero_name_raw': hero_clean,
                    'hero_id': normalize_hero_name(hero_clean), # Normalisasi penting!
                    'is_win': is_win
                })
    
    df_silver = pd.DataFrame(expanded_rows)
    
    # Simpan Silver (Data detail per hero per match)
    upload_df_to_minio(df_silver, BUCKET_NAME, "silver/user_history/user_hero_history.parquet", file_format='parquet')
    return df_silver

def process_user_gold():
    print("[User Pipeline] Processing Gold...")
    # Baca Silver
    df = read_df_from_minio(BUCKET_NAME, "silver/user_history/user_hero_history.parquet", file_format='parquet')
    if df is None: return

    # AGREGASI: Group by Hero untuk dapat statistik User
    # Inilah yang akan dibaca recommender.py
    df_agg = df.groupby('hero_id').agg(
        total_picks=('is_win', 'count'),
        total_wins=('is_win', 'sum')
    ).reset_index()
    
    df_agg['win_rate'] = df_agg['total_wins'] / df_agg['total_picks']
    df_agg['last_updated'] = get_timestamp()
    
    # Simpan Gold
    upload_df_to_minio(df_agg, BUCKET_NAME, "gold/user_history/user_hero_performance.parquet", file_format='parquet')
    print(f"[User Pipeline] DONE. Aggregated stats for {len(df_agg)} heroes.")

if __name__ == "__main__":
    process_user_bronze()
    process_user_silver()
    process_user_gold()