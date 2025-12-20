import pandas as pd
import sys, os
from datetime import datetime

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio

BUCKET_NAME = "mlbb-lakehouse"

def create_ml_features(df_silver_enriched):
    """
    Mengubah data Silver (Long Format) menjadi Gold (Wide Format) untuk ML.
    Satu baris = Satu Match.
    """
    print("--LOGIC: Creating ML Features (Pivot Data)")
    
    # Ambil hanya fase PICK (Ban biasanya dipisah atau diabaikan tergantung model)
    df_picks = df_silver_enriched[df_silver_enriched['phase'] == 'pick'].copy()
    
    # Kita butuh pivot agar menjadi: Match_ID | T1_Hero1 | ... | T1_Hero5 | T2_Hero1 | ... | Label
    # Asumsi: team_side 'left' = T1, 'right' = T2
    
    matches = []
    
    # Group by Match ID
    for match_id, group in df_picks.groupby('match_id'):
        row_data = {'match_id': match_id}
        
        # Ambil label pemenang dari salah satu baris (asumsi data konsisten)
        # Jika team_side 'left' menang, label=1, jika tidak label=0
        winner_left = group[group['team_side'] == 'left']['is_winner_team'].iloc[0]
        row_data['Label_Winner'] = 1 if winner_left else 0
        
        # Masukkan hero T1 (Left)
        left_heroes = group[group['team_side'] == 'left'].sort_values('order')['hero_name_normalized'].values
        for i, hero in enumerate(left_heroes):
            row_data[f'T1_Hero_{i+1}'] = hero
            # Bisa tambahkan fitur lain disini, misal: row_data[f'T1_H{i+1}_WinRate'] = ...
            
        # Masukkan hero T2 (Right)
        right_heroes = group[group['team_side'] == 'right'].sort_values('order')['hero_name_normalized'].values
        for i, hero in enumerate(right_heroes):
            row_data[f'T2_Hero_{i+1}'] = hero
            
        matches.append(row_data)
        
    df_features = pd.DataFrame(matches)
    
    # Opsi: Lakukan One-Hot Encoding jika XGBoost tidak support categorical string langsung
    # Atau biarkan string jika menggunakan model yang support categorical (seperti CatBoost)
    # Untuk contoh ini kita biarkan string atau lakukan encoding di step training
    
    return df_features

def create_hero_leaderboard(df_silver_enriched):
    """
    Membuat tabel agregasi untuk Dashboard / Streamlit
    """
    print("--LOGIC: Creating Hero Leaderboard")
    
    # Filter phase pick
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
    
    # 1. Load Silver Data
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet")
    if df_silver is None:
        print("Silver data not found!")
        return

    # 2. Create ML Training Data
    df_ml = create_ml_features(df_silver)
    upload_df_to_minio(df_ml, BUCKET_NAME, "gold/features_draft_model.parquet")
    print(f"DONE: ML Features saved ({len(df_ml)} matches)")

    # 3. Create Dashboard Data
    df_dashboard = create_hero_leaderboard(df_silver)
    upload_df_to_minio(df_dashboard, BUCKET_NAME, "gold/hero_leaderboard.parquet")
    print(f"DONE: Leaderboard saved ({len(df_dashboard)} heroes)")

if __name__ == "__main__":
    run_gold_pipeline()