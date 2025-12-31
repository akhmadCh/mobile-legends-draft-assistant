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

# data dashboard dan untuk rekomendasi
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
        pick_rate_avg=('pick_rate', 'mean'),
        tier_score=('tier_score', 'mean')
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

def run_gold3_pipeline():
    print('--- PIPELINE GOLD LAYER V3 (ENHANCED FEATURES) ---')
    
    # 1. Load Silver
    print('\n1. Loading Silver Data...')
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_enriched.parquet", file_format='parquet')
    # 2. load bronze untuk master data
    df_master = read_df_from_minio(BUCKET_NAME, "bronze/hero_stats/bronze_hero_stats.parquet", file_format='parquet')
    
    if df_silver is None: return
    if df_master is None: return

    # 3. Create Leaderboard (FULL HERO)
    print('\n2. Process Hero Leaderboard...')
    df_dashboard = create_hero_leaderboard(df_silver, df_master)
    upload_df_to_minio(df_dashboard, BUCKET_NAME, "gold/hero_leaderboard.parquet", file_format='parquet')
    print(f"DONE: Leaderboard saved ({len(df_dashboard)} heroes). Aamon harusnya ada sekarang.")

    print('\n3. Process Counter Matrix...')
    # 4. Create Counter Matrix
    # (Opsional) Load counter data jika terpisah
    df_counter_bronze = read_df_from_minio(BUCKET_NAME, "bronze/counter_hero/bronze_hero_counter.parquet", file_format='parquet')
    df_lookup = create_counter_matrix(df_counter_bronze)
    if not df_lookup.empty:
        upload_df_to_minio(df_lookup, BUCKET_NAME, "gold/hero_counter_lookup.parquet", file_format='parquet')
        print("DONE: Counter Lookup saved.")

    # 5. Pick Features
    print('\n5. Process Pick Features...')
    df_gold_picks = transform_gold_pick_features_v3(df_silver)
    
    # 6. Match Features (Role & Stats)
    print('\n6. Process Team Agregation (Role & Stats)...')
    df_gold_team = transform_gold_match_features_v3(df_gold_picks)
    
    # 7. Final Training Data (Team Strength)
    print('\n7. Process Final Training Data (Team Strength)...')
    df_training = transform_gold_match_level_v3(df_gold_team)
    
    # Simpan
    output_path = "gold/gold_training_dataset_v3.parquet"
    upload_df_to_minio(df_training, BUCKET_NAME, output_path, file_format='parquet')
    
    print(f"\nDONE: {output_path} ({len(df_training)} matches)")
    print("Sample Features:")
    print(df_training[['team_name_left', 'team_name_right', 'diff_team_strength', 'diff_counter', 'is_winner_team_left']].head())
    
    print('Sample 2 baris leaderboard hero:')
    print(df_dashboard.head(20))
    print('\nPreview Counter Lookup:')
    print(df_lookup.head(40))

if __name__ == "__main__":
    run_gold3_pipeline()