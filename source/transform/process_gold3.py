import pandas as pd
import numpy as np
import sys, os

# Menambahkan root project ke path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# helper functions
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
from source.utils.global_helper import get_timestamp

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "mlbb-lake"

def transform_gold_pick_features_v3(df_silver):
    """
    STEP 1: GOLD DRAFT PICK FEATURES
    Fokus: Mengambil data fase PICK saja dan normalisasi fitur.
    """
    print('--LOGIC: Filter Picks & Normalize Features')
    
    # Ambil hanya fase 'pick'
    df_picks = df_silver[df_silver['phase'] == 'pick'].copy()
    
    # Normalisasi Counter Score (biar range -1 s/d 1)
    max_abs_score = df_picks['counter_score'].abs().max()
    if max_abs_score == 0: max_abs_score = 1 
    df_picks['counter_score_norm'] = df_picks['counter_score'] / max_abs_score
    
    # Pilih kolom penting
    selected_cols = [
        'match_id', 'team_side', 'team_name', 'hero_name_normalized', 'is_winner_team',
        'win_rate', 'pick_rate', 'tier_score', 
        'counter_score', 'counter_score_norm',
        'role', 'lane', 'speciality'
    ]
    
    # Pastikan kolom ada
    existing_cols = [c for c in selected_cols if c in df_picks.columns]
    df_gold_picks = df_picks[existing_cols].copy()
    df_gold_picks['processed_at'] = get_timestamp()
    
    return df_gold_picks

def transform_gold_match_features_v3(df_gold_picks):
    """
    STEP 2: GOLD MATCH FEATURES (Role & Stats Agregasi)
    Fokus: Menambahkan fitur Role Balance & Agregasi Tim
    """
    print('--LOGIC: Agregasi data per Tim + Cek Role Balance')
    
    # --- HELPER: Cek Kelengkapan Role ---
    def check_roles(roles):
        # Logika: Tim harus punya lini depan (Tank/Fighter) DAN lini belakang (MM/Mage)
        has_frontline = any(r in ['Tank', 'Fighter', 'Support'] for r in roles)
        has_damage = any(r in ['Marksman', 'Mage', 'Assassin'] for r in roles)
        return 1 if (has_frontline and has_damage) else 0

    # Groupby per Tim per Match
    grouped = df_gold_picks.groupby(['match_id', 'team_side'])
    aggregated_data = []
    
    for (match_id, side), group in grouped:
        roles = group['role'].tolist()
        
        row = {
            'match_id': match_id,
            'team_side': side,
            'team_name': group['team_name'].iloc[0],
            'is_winner_team': group['is_winner_team'].iloc[0],
            
            # Statistik Rata-rata
            'avg_win_rate_team': group['win_rate'].mean(),
            'avg_meta_score_team': group['tier_score'].mean(),
            'avg_counter_score_team': group['counter_score'].mean(),
            
            # Validasi Data
            'total_heroes_count': len(group),
            
            # --- FITUR BARU: Role Balance ---
            'is_role_balanced': check_roles(roles)
        }
        aggregated_data.append(row)
        
    df_match_features = pd.DataFrame(aggregated_data)
    
    # Filter Data Bersih (Harus 5 hero)
    df_clean = df_match_features[df_match_features['total_heroes_count'] == 5].copy()
    
    dropped = len(df_match_features) - len(df_clean)
    if dropped > 0:
        print(f"WARNING: {dropped} tim dibuang karena data hero tidak lengkap (<5).")
        
    return df_clean

def transform_gold_match_level_v3(df_team):
    """
    STEP 3: MATCH LEVEL (Left vs Right) + TEAM STRENGTH
    """
    print('--LOGIC: Gabung Left vs Right & Hitung Team Strength')

    left = df_team[df_team['team_side'] == 'left']
    right = df_team[df_team['team_side'] == 'right']

    df_match = pd.merge(left, right, on='match_id', suffixes=('_left', '_right'))

    # --- FITUR BARU: TEAM STRENGTH (TARGET ENCODING) ---
    # Hitung WR rata-rata tiap tim di seluruh dataset historis ini
    # (Semakin sering menang, skor strength makin tinggi)
    team_stats = df_team.groupby('team_name')['is_winner_team'].mean().to_dict()
    
    # Map ke dataset
    # Jika tim baru/tidak dikenal, kasih nilai netral 0.5
    df_match['team_left_strength'] = df_match['team_name_left'].map(team_stats).fillna(0.5)
    df_match['team_right_strength'] = df_match['team_name_right'].map(team_stats).fillna(0.5)
    
    # Hitung Selisih (Diff Features) untuk XGBoost
    df_match['diff_team_strength'] = df_match['team_left_strength'] - df_match['team_right_strength']
    df_match['diff_counter'] = df_match['avg_counter_score_team_left'] - df_match['avg_counter_score_team_right']
    df_match['diff_meta'] = df_match['avg_meta_score_team_left'] - df_match['avg_meta_score_team_right']
    df_match['diff_win_rate'] = df_match['avg_win_rate_team_left'] - df_match['avg_win_rate_team_right']
    df_match['diff_role_balance'] = df_match['is_role_balanced_left'] - df_match['is_role_balanced_right']

    df_match['processed_at'] = get_timestamp()

    return df_match

def run_gold3_pipeline():
    print('--- PIPELINE GOLD LAYER V3 (ENHANCED FEATURES) ---')
    
    # 1. Load Silver
    print('\n1. Loading Silver Data...')
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    if df_silver is None: return

    # 2. Pick Features
    print('\n2. Process Pick Features...')
    df_gold_picks = transform_gold_pick_features_v3(df_silver)
    
    # 3. Match Features (Role & Stats)
    print('\n3. Process Team Agregation (Role & Stats)...')
    df_gold_team = transform_gold_match_features_v3(df_gold_picks)
    
    # 4. Final Training Data (Team Strength)
    print('\n4. Process Final Training Data (Team Strength)...')
    df_training = transform_gold_match_level_v3(df_gold_team)
    
    # Simpan
    output_path = "gold/gold_training_dataset_v3.parquet"
    upload_df_to_minio(df_training, BUCKET_NAME, output_path, file_format='parquet')
    
    print(f"\n✅ DONE: {output_path} ({len(df_training)} matches)")
    print("Sample Features:")
    print(df_training[['team_name_left', 'team_name_right', 'diff_team_strength', 'diff_counter', 'is_winner_team_left']].head())

if __name__ == "__main__":
    run_gold3_pipeline()