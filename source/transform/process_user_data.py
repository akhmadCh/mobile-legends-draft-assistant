import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp
from source.utils.helper_bronze import normalize_hero_name

BUCKET_NAME = "mlbb-lake"

# --- 1. BRONZE LAYER: Raw Ingestion ---
def process_user_bronze():
    print("[User Pipeline] Processing Bronze...")
    
    df = read_df_from_minio(BUCKET_NAME, "raw/user_history/match_history_user.csv", file_format='csv')
    if df is None or df.empty:
        print("No raw data found.")
        return None

    # metadata manual
    df['ingested_at'] = get_timestamp()

    upload_df_to_minio(df, BUCKET_NAME, "bronze/user_history/user_match_history.parquet", file_format='parquet')
    print("[User Pipeline] Bronze Saved.")
    return df

# --- 2. SILVER LAYER: Cleaning & Formatting ---
def process_user_silver():
    print("[User Pipeline] Processing Silver...")
    
    df = read_df_from_minio(BUCKET_NAME, "bronze/user_history/user_match_history.parquet", file_format='parquet')
    if df is None or df.empty: return None

    # cleaning, jadi nilai 1/0
    df['is_win'] = df['result'].apply(lambda x: 1 if str(x).strip().lower() in ['win', 'victory'] else 0)

    # normalisasi nama hero
    df['user_hero_id'] = df['user_hero'].apply(lambda x: normalize_hero_name(str(x)))

    # parsing tim (String -> List) "Layla, Tigreal" -> ['layla', 'tigreal']
    def parse_team_list(team_str):
        if pd.isna(team_str): return []
        # split koma, strip spasi, lalu normalize
        return [normalize_hero_name(h.strip()) for h in str(team_str).split(',') if h.strip()]

    df['my_team_list'] = df['my_team'].apply(parse_team_list)

    df_silver = df[['timestamp', 'user_hero_id', 'my_team_list', 'is_win', 'ingested_at']]
    
    upload_df_to_minio(df_silver, BUCKET_NAME, "silver/user_history/user_match_enriched.parquet", file_format='parquet')
    print("[User Pipeline] Silver Saved.")
    return df_silver

# --- 3. GOLD LAYER: Aggregation ---
def process_user_gold():
    print("[User Pipeline] Processing Gold...")
    
    df = read_df_from_minio(BUCKET_NAME, "silver/user_history/user_match_enriched.parquet", file_format='parquet')
    if df is None or df.empty: return

    # PERSONAL STATS
    # hitung performa User Hero
    # group by id 'user_hero_id'
    df_personal = df.groupby('user_hero_id').agg(
        total_picks=('is_win', 'count'),
        total_wins=('is_win', 'sum')
    ).reset_index()
    
    df_personal['win_rate'] = df_personal['total_wins'] / df_personal['total_picks']
    df_personal['hero_id'] = df_personal['user_hero_id'] # rename nama hero agar konsisten
    
    upload_df_to_minio(df_personal, BUCKET_NAME, "gold/user_history/user_hero_performance.parquet", file_format='parquet')
    print(f"Saved Personal Stats: {len(df_personal)} heroes")

    # SYNERGY STATS
    # (explode) list tim menjadi baris agar bisa grouping
    # 2. [User: Layla, Teammate: Tigreal, Win: 1] -> data sinergi

    df_exploded = df.explode('my_team_list').rename(columns={'my_team_list': 'teammate_id'})
    
    # filter, hapus jika teammate == user_hero (tidak menghitung sinergi hero sendiri)
    df_synergy_clean = df_exploded[df_exploded['teammate_id'] != df_exploded['user_hero_id']]

    # agregasi data sinergi
    df_synergy_agg = df_synergy_clean.groupby('teammate_id').agg(
        matches_together=('is_win', 'count'),
        wins_together=('is_win', 'sum')
    ).reset_index()

    df_synergy_agg['synergy_wr'] = df_synergy_agg['wins_together'] / df_synergy_agg['matches_together']
    df_synergy_agg['hero_id'] = df_synergy_agg['teammate_id'] # Rename untuk konsistensi

    upload_df_to_minio(df_synergy_agg, BUCKET_NAME, "gold/user_history/user_team_synergy.parquet", file_format='parquet')
    print(f"Saved Synergy Stats: {len(df_synergy_agg)} teammates")

if __name__ == "__main__":
    process_user_bronze()
    process_user_silver()
    process_user_gold()