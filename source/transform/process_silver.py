import pandas as pd
import numpy as np
import sys, os
import ast 

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp
from source.utils.helper_silver import calculate_avg_counter_score

BUCKET_NAME = "mlbb-lakehouse"

def ensure_list(val):
    """
    Memaksa input menjadi list python standar [].
    Menangani: numpy array, string representation, dan NaN.
    """
    # 1. Jika NaN / None -> List kosong
    if val is None:
        return []
    if isinstance(val, float) and np.isnan(val):
        return []
    
    # 2. Jika Numpy Array -> Convert to list
    if isinstance(val, np.ndarray):
        return val.tolist()
        
    # 3. Jika String "['a', 'b']" -> Parse jadi list
    if isinstance(val, str):
        try:
            # Coba parsing aman
            clean_str = val.replace('\n', '').strip()
            # Handle format numpy string yg aneh (e.g. "['a' 'b']") yg gak ada koma
            if "' '" in clean_str or '" "' in clean_str:
                clean_str = clean_str.replace("' '", "', '").replace('" "', '", "')
            return ast.literal_eval(clean_str)
        except:
            return [] # Gagal parse
            
    # 4. Jika sudah List -> Kembalikan
    if isinstance(val, list):
        return val
        
    return []
# -------------------------------------------------

def transform_explode_draft(df_matches):
    print('--LOGIC: Explode drafts')
    exploded_rows = []
    
    # Kita apply ensure_list dulu ke semua kolom target agar aman
    target_cols = ['left_picks_normalized', 'left_bans_normalized', 'right_picks_normalized', 'right_bans_normalized']
    for col in target_cols:
        if col in df_matches.columns:
            df_matches[col] = df_matches[col].apply(ensure_list)

    for idx, row in df_matches.iterrows():
        match_id = idx + 1
        winner = row['winner_match']
        
        sides = [
            ('left', row['team_left'], row['left_picks_normalized'], row['left_bans_normalized']),
            ('right', row['team_right'], row['right_picks_normalized'], row['right_bans_normalized'])
        ]
        
        for side_name, team_name, picks, bans in sides:
            is_winner = (str(winner).strip().lower() == str(team_name).strip().lower())
            
            # Karena sudah diapply ensure_list, picks PASTI berupa list (bisa kosong)
            for i, hero in enumerate(picks):
                exploded_rows.append({
                    'match_id': match_id,
                    'region': row['region'],
                    'tournament': row['tournament'],
                    'team_side': side_name,
                    'team_name': team_name,
                    'phase': 'pick',
                    'order': i + 1,
                    'hero_name_normalized': hero,
                    'is_winner_team': is_winner,
                    'source_file': row['source_file'],
                    'ingested_at': row['ingested_at']
                })
            
            for i, hero in enumerate(bans):
                exploded_rows.append({
                    'match_id': match_id,
                    'region': row['region'],
                    'tournament': row['tournament'],
                    'team_side': side_name,
                    'team_name': team_name,
                    'phase': 'ban',
                    'order': i + 1,
                    'hero_name_normalized': hero,
                    'is_winner_team': is_winner,
                    'source_file': row['source_file'],
                    'ingested_at': row['ingested_at']
                })
        
    return pd.DataFrame(exploded_rows)

def transform_calculate_scores(df_matches, df_counter):
    """
    TABLE 1: Menghitung skor counter terhadap tim lawan
    """
    print('--LOGIC: Hitung skor counter')
    
    counter_dict = {}
    if df_counter is not None:
        for hero, counter, score in zip(df_counter['hero_name_normalized'], df_counter['counter_name_normalized'], df_counter['score']):
            # paksa string & lowercase saat bikin kamus
            #  h = str(hero).strip().lower()
            #  c = str(counter).strip().lower()
            #  counter_dict[(h, c)] = score
        
            counter_dict[(hero, counter)] = score
        
    match_counter_scores = []
    
    # hitung skor per match
    for idx, row in df_matches.iterrows():
        match_id = idx + 1
        left_heroes = row.get('left_picks_normalized', [])
        right_heroes = row.get('right_picks_normalized', [])
        
        # validasi tipe data list
        if not isinstance(left_heroes, (list, np.ndarray)): left_heroes = []
        if not isinstance(right_heroes, (list, np.ndarray)): right_heroes = []

        # skor tim kiri (vs Kanan)
        for hero_raw, hero_clean in zip(left_heroes, left_heroes):
            score = calculate_avg_counter_score(hero_clean, right_heroes, counter_dict)
        
        match_counter_scores.append({
            'match_id': match_id, 
            'team_side': 'left', 
            'hero_name_normalized': hero_raw, 
            'counter_score': score
        })
        
    # skor tim kanan (vs Kiri)
    for hero_raw, hero_clean in zip(right_heroes, right_heroes):
        score = calculate_avg_counter_score(hero_clean, left_heroes, counter_dict)
        match_counter_scores.append({
            'match_id': match_id, 
            'team_side': 'right', 
            'hero_name_normalized': hero_raw, 
            'counter_score': score
        })

    return pd.DataFrame(match_counter_scores)

def transform_enrich_draft(df_draft, df_stats, df_meta, df_scores):
    print("--LOGIC: Merge semua data sources")
    
    # Merge Stats
    df_enriched = pd.merge(df_draft, df_stats[['hero_name_normalized', 'win_rate', 'pick_rate', 'ban_rate', 'role', 'lane', 'speciality']], how='left', on='hero_name_normalized')
    
    # Merge Meta
    df_enriched = pd.merge(df_enriched, df_meta[['hero_name_normalized', 'tier_score', 'score']], how='left', on='hero_name_normalized', suffixes=('', '_meta'))
    
    # Merge Counter Scores
    df_enriched = pd.merge(df_enriched, df_scores, how='left', on=['match_id', 'team_side', 'hero_name_normalized'])
    
    fill_values = {'win_rate': 0.0, 'pick_rate': 0.0, 'tier_score': 0, 'counter_score': 0.0, 'role': 'Unknown', 'lane': 'Unknown', 'speciality': 'Unknown'}
    df_final = df_enriched.fillna(value=fill_values)
    df_final['processed_at'] = get_timestamp()
    
    return df_final, df_enriched

def generate_quality_report(df_raw_enriched):
    # Skip logic complex utk sementara biar cepat
    return None

def run_silver_pipeline():
    print('--- PIPELINE TRASNFORMING SILVER LAYER ---')
    
    # 1. Load Data
    # Penting: file_format='parquet' karena kamu simpan parquet
    df_matches = read_df_from_minio(BUCKET_NAME, "bronze/tournament_matches/bronze_mpl_matches.parquet", file_format='parquet')
    
    if df_matches is None or len(df_matches) == 0:
        print("Bronze data not found!")
        return
        
    print(f"   [INFO] Loaded {len(df_matches)} matches from Bronze.")

    df_stats = read_df_from_minio(BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
    df_meta = read_df_from_minio(BUCKET_NAME, "bronze/meta/bronze_hero_meta.parquet", file_format='parquet')
    df_counter = read_df_from_minio(BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')

    # 2. Transform Table 1
    print("\n2/5 Create Table 1: Silver Draft Heroes...")
    df_draft_heroes = transform_explode_draft(df_matches)
    
    # CEK HASIL DISINI
    print(f"   [CHECK] Generated {len(df_draft_heroes)} rows (Expected approx: {len(df_matches)*10})")
    
    upload_df_to_minio(df_draft_heroes, BUCKET_NAME, "silver/silver_draft_heroes.parquet", file_format='parquet')

    # 3. Calculate Scores
    print("\n3/5 Calculate Counter Scores...")
    df_scores = transform_calculate_scores(df_matches, df_counter)
    
    # 4. Enrich
    print("\n4/5 Create Table 2: Silver Enriched Data...")
    df_final, _ = transform_enrich_draft(df_draft_heroes, df_stats, df_meta, df_scores)
    
    upload_df_to_minio(df_final, BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    print(f"--DONE: silver_draft_enriched.parquet ({len(df_final)} rows)")
    
    # --- STEP 4: PREVIEW DATA ---
    print('\n4/4 Preview Data untuk verifikasi:')
    print('Sample 2 baris features tim:')
    print(df_final[['team_name', 'team_side', 'hero_name_normalized', 'pick_rate', 'win_rate', 'tier_score', 'counter_score', 'is_winner_team']].head(40))
    
    print("\nSILVER PIPELINE COMPLETED")

if __name__ == "__main__":
    run_silver_pipeline()