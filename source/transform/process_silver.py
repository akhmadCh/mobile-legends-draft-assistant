import pandas as pd
import numpy as np
import sys, os
from datetime import datetime

# Menambahkan root project ke path agar bisa import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# helper functions
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp
from source.utils.helper_silver import calculate_avg_counter_score

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "mlbb-lakehouse"

def transform_explode_draft(df_matches):
   print('--LOGIC: Explode drafts')
   # explode the data (bans, picks)
   
   # create table 1: silver_draft_heroes
   print("2/5 Silver Table 1 'silver_draft_heroes': ")
   print('1 table 1 hero')
   
   exploded_rows = []
   
   # iterate manually per match for easy controll
   for idx, row in df_matches.iterrows():
      match_id = idx + 1
      winner = row['winner_match']
      
      # helper untuk tiap sisi (left/right)
      sides = [
         ('left', row['team_left'], row['left_picks_normalized'], row['left_bans_normalized']),
         ('right', row['team_right'], row['right_picks_normalized'], row['right_bans_normalized'])
      ]
      
      for side_name, team_name, picks, bans in sides:
         is_winner = (winner == team_name)
         
         # proses picks
         # pakai enum agar mendapatkan urutan 1, 2, 3
         if isinstance(picks, (list, np.ndarray)):
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
         
         # proses bans
         if isinstance(bans, (list, np.ndarray)):
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
      for hero in left_heroes:
         score = calculate_avg_counter_score(hero, right_heroes, counter_dict)
         match_counter_scores.append({'match_id': match_id, 'team_side': 'left', 'hero_name_normalized': hero, 'counter_score': score})
         
      # skor tim kanan (vs Kiri)
      for hero in right_heroes:
         score = calculate_avg_counter_score(hero, left_heroes, counter_dict)
         match_counter_scores.append({'match_id': match_id, 'team_side': 'right', 'hero_name_normalized': hero, 'counter_score': score})

   return pd.DataFrame(match_counter_scores)

def transform_enrich_draft(df_draft, df_stats, df_meta, df_scores):
   """
   Table 2: Menggabungkan draft hero dengan stats, meta, dan counter scores.
   """
   print("--LOGIC: Merge semua data sources")
   
   # 1. join dengan stats global (Winrate, etc)
   # left join ke data Draft yg ada di MPL (fokus data di MPL)
   df_enriched = pd.merge(
      df_draft,
      df_stats[['hero_name_normalized', 'win_rate', 'pick_rate', 'ban_rate', 'speciality']],
      how='left',
      on='hero_name_normalized',
   )
   
   # 2. join dengan meta
   df_enriched = pd.merge(
      df_enriched,
      df_meta[['hero_name_normalized', 'tier_score', 'score']], # score = meta score
      how='left',
      on='hero_name_normalized',
      suffixes=('', '_meta')
   )
   
   # 3. join dengan counter score sebelumnya (yang baru dihitung)
   df_enriched = pd.merge(
      df_enriched,
      df_scores,
      how='left',
      on=['match_id', 'team_side', 'hero_name_normalized'],
   )
   
   # 4. fill NA dengan Default Value agar aman masuk db/model
   fill_values = {
      'win_rate': 0.0, 
      'pick_rate': 0.0,
      'tier_score': 0, 
      'counter_score': 0.0,
      'speciality': 'Unknown'
   }
   df_final = df_enriched.fillna(value=fill_values)
   df_final['processed_at'] = get_timestamp()
   
   return df_final, df_enriched # df_enriched (yg ada NaN) untuk keperluan cek DQ

def generate_quality_report(df_raw_enriched):
   """
   Table 3: Mengecek anomali atau data hilang sebelum fillna.
   """
   print("--Logic: Generate data untuk reporting")
   dq_issues = []
   
   # cek 1: hero draft yang tidak punya data stats master
   missing_stats = df_raw_enriched[df_raw_enriched['win_rate'].isnull()]
   for _, row in missing_stats.iterrows():
      dq_issues.append({
         'match_id': row['match_id'],
         'hero_name_normalized': row['hero_name_normalized'],
         'issue_type': 'HERO_NOT_FOUND_IN_MASTER',
         'description': 'Win Rate stats missing in Master Data',
         'detected_at': get_timestamp()
      })
      
   # cek 2: counter score yang missing (khusus phase pick)
   missing_counter = df_raw_enriched[
      (df_raw_enriched['phase'] == 'pick') & 
      (df_raw_enriched['counter_score'].isnull())
   ]
   for _, row in missing_counter.iterrows():
      dq_issues.append({
         'match_id': row['match_id'],
         'hero_name_normalized': row['hero_name_normalized'],
         'issue_type': 'COUNTER_SCORE_MISSING',
         'description': 'Failed to calculate counter score',
         'detected_at': get_timestamp()
      })
      
   return pd.DataFrame(dq_issues) if dq_issues else None

def run_silver_pipeline():
   print('--- PIPELINE TRASNFORMING SILVER LAYER ---')
   # load semua data dari bronze
   print('1/5 Loading All Bronze Data:')
   df_matches = read_df_from_minio(BUCKET_NAME, "bronze/tournament_matches/bronze_mpl_matches.parquet", file_format='parquet')
   df_stats = read_df_from_minio(BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
   df_meta = read_df_from_minio(BUCKET_NAME, "bronze/meta/bronze_hero_meta.parquet", file_format='parquet')
   df_counter = read_df_from_minio(BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')

   # validate the bronze data
   if df_matches is None or df_stats is None or df_meta is None or df_counter is None:
      print('--ERROR: Bronze data is not found...')
      return

   # --- STEP 2: CREATE TABLE 1 (DRAFT HEROES) ---
   print("\n2/5 Create Table 1: Silver Draft Heroes...")
   df_draft_heroes = transform_explode_draft(df_matches)
   upload_df_to_minio(df_draft_heroes, BUCKET_NAME, "silver/silver_draft_heroes.parquet", file_format='parquet')
   print(f"DONE: silver_draft_heroes.parquet ({len(df_draft_heroes)} rows)")

   # --- STEP 3: CALCULATE SCORES ---
   print("\n3/5 Calculate Counter Scores...")
   df_scores = transform_calculate_scores(df_matches, df_counter)
   
   # --- STEP 4: CREATE TABLE 2 (ENRICHED) ---
   print("\n4/5 Create Table 2: Silver Enriched Data...")
   df_final, df_raw_enriched = transform_enrich_draft(df_draft_heroes, df_stats, df_meta, df_scores)
   upload_df_to_minio(df_final, BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
   print(f"--DONE: silver_draft_enriched.parquet ({len(df_final)} rows)")
   
   # --- STEP 5: CREATE TABLE 3 (DATA QUALITY) ---
   print("\n5/5 Create Table 3: Data Quality Report...")
   df_dq = generate_quality_report(df_raw_enriched)
   
   if df_dq is not None:
      upload_df_to_minio(df_dq, BUCKET_NAME, "silver/silver_data_quality.parquet", file_format='parquet')
      print(f"--SAVED: silver_data_quality.parquet ({len(df_dq)} issues found)")
   else:
      print("Perfect Data! No issues found.")

   print("\nSILVER PIPELINE COMPLETED")

if __name__ == "__main__":
   run_silver_pipeline()