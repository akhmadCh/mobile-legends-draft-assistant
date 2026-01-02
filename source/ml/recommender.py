import pandas as pd
import os
import sys
import re
import numpy as np
from datetime import datetime

# Setup path agar bisa import helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Coba import fungsi MinIO. 
# Jika error (misal dijalankan lokal tanpa minio), pakai dummy agar tidak crash.
try:
    from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("[WARNING] MinIO Helper not found. Running in Offline Mode.")

BUCKET_NAME = "mlbb-lake"

# --- KONFIGURASI PATH BARU ---
# Path Global (Data Hero Umum)
GLOBAL_STATS_PATH = "gold/hero_leaderboard.parquet"
COUNTER_DATA_PATH = "gold/hero_counter_lookup.parquet"

# Path User Data (Arsitektur Baru)
# 1. Write: Simpan data mentah match baru ke RAW (CSV)
RAW_HISTORY_PATH = "raw/user_history/match_history_user.csv" 
# 2. Read: Baca statistik user yang sudah matang dari GOLD (Parquet)
GOLD_USER_STATS_PATH = "gold/user_history/user_hero_performance.parquet"
GOLD_USER_SYNERGY_PATH = "gold/user_history/user_team_synergy.parquet"

class DraftRecommender:
    def __init__(self):
        print("--- [INFO] Initializing Draft Recommender (ETL Architecture) ---")
        
        # 1. Load Data Statistik Global & Counter
        if MINIO_AVAILABLE:
            self.df_stats = read_df_from_minio(BUCKET_NAME, GLOBAL_STATS_PATH, file_format='parquet')
            self.df_counters = read_df_from_minio(BUCKET_NAME, COUNTER_DATA_PATH, file_format='parquet')
            
            # 2. Load User Stats dari GOLD Layer
            self.df_user_perf = read_df_from_minio(BUCKET_NAME, GOLD_USER_STATS_PATH, file_format='parquet')
            
            # data sinergi
            self.df_synergy = read_df_from_minio(BUCKET_NAME, GOLD_USER_SYNERGY_PATH, file_format='parquet')
            
            if self.df_user_perf is None or self.df_user_perf.empty:
                print("[INFO] User Gold data not found. New user or pipeline hasn't run.")
                self.df_user_perf = pd.DataFrame(columns=['hero_id', 'total_picks', 'win_rate'])
            else:
                print(f"[INFO] Loaded User Stats for {len(self.df_user_perf)} heroes from Gold.")
            
            if self.df_synergy is None: self.df_synergy = pd.DataFrame()
            
        else:
            # fallback jika MinIO mati (misal pakai CSV lokal/offline)
            self.df_stats = pd.DataFrame()
            self.df_counters = pd.DataFrame()
            self.df_user_perf = pd.DataFrame()
        
        # 3. (Cleaning & Formatting)
        self._prepare_data()

    def _normalize_name(self, name):
        """Membersihkan nama hero untuk pencarian (hapus spasi/simbol, lowercase)."""
        if pd.isna(name) or name is None: return ""
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()

    # MANAJEMEN DATA USER (WRITE RAW -> READ GOLD)
    def save_match_result(self, my_team, enemy_team, result_status, user_hero_played, username):
        """
        Menyimpan hasil match dengan format baru:
        Menambahkan kolom 'user_hero' agar sistem tahu mana yang dimainkan user.
        """
        if not MINIO_AVAILABLE:
            print("[ERROR] MinIO not available.")
            return False

        try:
            # Format list ke string
            my_team_str = ",".join([str(x) for x in my_team if x])
            enemy_team_str = ",".join([str(x) for x in enemy_team if x])
            
            new_data = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'username': str(username).strip().lower(), # <--- KOLOM BARU
                'my_team': my_team_str,
                'enemy_team': enemy_team_str,
                'result': result_status,
                'user_hero': str(user_hero_played)
            }
            
            new_row = pd.DataFrame([new_data])
            
            # Load existing, concat, save (seperti biasa)
            existing_df = read_df_from_minio(BUCKET_NAME, RAW_HISTORY_PATH, file_format='csv')
            
            if existing_df is not None and not existing_df.empty:
                final_df = pd.concat([existing_df, new_row], ignore_index=True)
            else:
                final_df = new_row
            
            upload_df_to_minio(final_df, BUCKET_NAME, RAW_HISTORY_PATH, file_format='csv')
            return True
            
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
            return False

    def get_user_hero_stats(self, hero_name, username):
        """
        Mengambil statistik hero KHUSUS untuk username tertentu.
        """
        if self.df_user_perf.empty:
            return 0, 0.0
        
        hero_clean = self._normalize_name(hero_name)
        user_clean = str(username).strip().lower()
        
        # --- PERBAIKAN: Cek apakah kolom 'username' ada ---
        if 'username' in self.df_user_perf.columns:
            # Jika kolom ada, filter berdasarkan User DAN Hero
            row = self.df_user_perf[
                (self.df_user_perf['hero_id'] == hero_clean) & 
                (self.df_user_perf['username'] == user_clean)
            ]
        else:
            # Jika kolom BELUM ada (Data Lama), anggap ini data milik 'adri' (default)
            # Jadi kalau login sebagai 'adri', dia baca data lama. Kalau user lain, 0.
            if user_clean == 'adri': 
                row = self.df_user_perf[self.df_user_perf['hero_id'] == hero_clean]
            else:
                return 0, 0.0
        
        if not row.empty:
            picks = int(row.iloc[0]['total_picks'])
            wr = float(row.iloc[0]['win_rate'])
            return picks, wr
            
        return 0, 0.0

    # DATA (GLOBAL & COUNTER)
    def _prepare_data(self):
        """Merapikan data global dan counter agar siap pakai."""
        # 1. Stats Global
        if self.df_stats is not None and not self.df_stats.empty:
            rename_map = {
                'hero_name_raw': 'hero_name', 
                'win_rate': 'win_rate',
                'ban_rate': 'ban_rate',
                'pick_rate': 'pick_rate',
                'lane': 'lane',
                'role': 'role'
            }
            # Rename kolom jika perlu
            self.df_stats = self.df_stats.rename(columns={k:v for k,v in rename_map.items() if k in self.df_stats.columns})
            
            # Buat kolom kunci normalisasi untuk join/pencarian
            self.df_stats['join_key'] = self.df_stats['hero_name'].apply(self._normalize_name)
            
            # Pastikan numerik float aman
            for col in ['win_rate', 'ban_rate']:
                if col in self.df_stats.columns:
                    self.df_stats[col] = self.df_stats[col].fillna(0.0).astype(float)
        else:
            self.df_stats = pd.DataFrame()

        # 2. Counter Data
        if self.df_counters is not None and not self.df_counters.empty:
            rename_map = {
                'Target_Name': 'target',
                'Counter_Name': 'counter',
                'Score': 'score'
            }
            self.df_counters = self.df_counters.rename(columns={k:v for k,v in rename_map.items() if k in self.df_counters.columns})
            
            if 'target' in self.df_counters.columns:
                self.df_counters['target_key'] = self.df_counters['target'].apply(self._normalize_name)
                self.df_counters['counter_key'] = self.df_counters['counter'].apply(self._normalize_name)
        else:
            self.df_counters = pd.DataFrame()

    def get_hero_info(self, hero_name):
         if self.df_stats.empty: return None
         search_key = self._normalize_name(hero_name)
         row = self.df_stats[self.df_stats['join_key'] == search_key]
         return row.iloc[0] if not row.empty else None

    def get_team_missing_roles(self, current_team):
        """Menganalisa role apa yang belum ada di tim."""
        filled_lanes = []
        for hero in current_team:
            if not hero: continue
            info = self.get_hero_info(hero)
            if info is not None and 'lane' in info:
                raw_lane = str(info['lane']).lower()
                filled_lanes.append(raw_lane)
        
        filled_string = " ".join(filled_lanes)
        needed_roles = []
        
        # Logika sederhana cek role standard
        if "exp" not in filled_string: needed_roles.append("Exp Lane")
        if "gold" not in filled_string: needed_roles.append("Gold Lane")
        if "mid" not in filled_string: needed_roles.append("Mid Lane")
        if "roam" not in filled_string: needed_roles.append("Roamer")
        if "jung" not in filled_string: needed_roles.append("Jungler")
        
        return needed_roles

    # REKOMENDASI (BAN & PICK)

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        """Rekomendasi Ban: Fokus pada Meta & Counter."""
        if self.df_stats.empty: return []
        
        # Filter hero yang sudah tidak tersedia
        all_unavailable = (my_team or []) + (enemy_team or []) + (banned_heroes or [])
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable if h])
        
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        if candidates.empty: return []

        # 1. Base Score: Ban Rate Global
        if 'ban_rate' in candidates.columns:
            candidates['ban_score'] = candidates['ban_rate'] * 100
            candidates['reasons'] = candidates['ban_rate'].apply(lambda x: [f"⚠️ Sering diban ({x:.1f}%)"])
        else:
            candidates['ban_score'] = 0
            candidates['reasons'] = [[] for _ in range(len(candidates))]

        # 2. Counter Logic: Ban hero yang mengancam pick kita (jika ada)
        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                if not my_hero: continue
                my_key = self._normalize_name(my_hero)
                # Cari hero yang skor counternya tinggi terhadap hero kita
                threats = self.df_counters[
                    (self.df_counters['target_key'] == my_key) & 
                    (self.df_counters['score'] > 2.0)
                ]
                
                for _, row in threats.iterrows():
                    counter_key = row['counter_key']
                    match_idx = candidates.index[candidates['join_key'] == counter_key].tolist()
                    for idx in match_idx:
                        candidates.at[idx, 'ban_score'] += 80 # Prioritas tinggi
                        candidates.at[idx, 'reasons'].insert(0, f"🛑 Counter berat {my_hero}")

        # Ambil Top 25
        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(25)
        
        results = []
        for _, row in recommendations.iterrows():
            reason_str = " • ".join(list(dict.fromkeys(row['reasons']))[:2])
            results.append({'hero': row['hero_name'], 'reason': reason_str})
            
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes, username):
        """
        Rekomendasi Pick dengan Rumus:
        Skor = (Data Eksternal * 40%) + (Kebutuhan Tim * 30%) + (User Performance * 30%)
        """
        if self.df_stats.empty: return []
        
        all_unavailable = (my_team or []) + (enemy_team or []) + (banned_heroes or [])
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable if h])
        
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        
        if candidates.empty: return []

        # Inisialisasi kolom
        candidates['score_external'] = 0.0
        candidates['score_team'] = 0.0
        candidates['score_user'] = 0.0
        candidates['reasons'] = [[] for _ in range(len(candidates))]

        # 1. KOMPONEN DATA EKSTERNAL (40%) - Meta & Counter
        if 'win_rate' in candidates.columns:
            # Win rate 50% -> skor 50. 
            candidates['score_external'] = candidates['win_rate'] * 100
        else:
            candidates['score_external'] = 50.0

        # Logika Counter vs Musuh
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                if not enemy: continue
                enemy_key = self._normalize_name(enemy)
                
                # Bonus: Hero ini meng-counter musuh
                good_counters = self.df_counters[self.df_counters['target_key'] == enemy_key]
                for _, row in good_counters.iterrows():
                    match_idx = candidates.index[candidates['join_key'] == row['counter_key']].tolist()
                    for idx in match_idx:
                        score = float(row['score'])
                        if score >= 2.0:
                            candidates.at[idx, 'score_external'] += 30 
                            candidates.at[idx, 'reasons'].append(f"⚔️ Hard Counter {enemy}")
                        elif score >= 1.0:
                            candidates.at[idx, 'score_external'] += 15
                            candidates.at[idx, 'reasons'].append(f"🛡️ Counter {enemy}")

                # Penalty: Hero ini lemah lawan musuh
                threats = self.df_counters[self.df_counters['counter_key'] == enemy_key]
                for _, row in threats.iterrows():
                     match_idx = candidates.index[candidates['join_key'] == row['target_key']].tolist()
                     for idx in match_idx:
                         candidates.at[idx, 'score_external'] -= 20
                         candidates.at[idx, 'reasons'].append(f"⚠️ Lemah vs {enemy}")

        # 2. KOMPONEN KEBUTUHAN TIM (30%) - Role Filling
        if my_team:
            missing_roles = self.get_team_missing_roles(my_team)
            if missing_roles:
                for idx, row in candidates.iterrows():
                    if 'lane' in row:
                        hero_lane = str(row['lane']).lower()
                        for role in missing_roles:
                            keyword = role.split()[0].lower() # e.g. "gold"
                            if keyword in hero_lane:
                                candidates.at[idx, 'score_team'] = 100.0
                                candidates.at[idx, 'reasons'].insert(0, f"✅ Isi {role}")
                                break
        else:
            # First pick bebas
            candidates['score_team'] = 100.0

        # 3. KOMPONEN KECOCOKAN USER (30%) - Data dari Gold Layer
        for idx, row in candidates.iterrows():
            hero_name = row['hero_name']
            
            # Panggil fungsi yang sekarang membaca dari Gold Parquet
            u_pick, u_wr = self.get_user_hero_stats(hero_name, username)
            
            user_score = 50.0 # Default netral
            
            if u_pick > 0:
                if u_wr > 0.6: # Jago (>60% WR)
                    user_score = 100.0
                    candidates.at[idx, 'reasons'].insert(0, f"🌟 Hero Andalan (WR {u_wr:.0%})")
                elif u_pick >= 3: # Comfort Pick
                    user_score = 80.0
                    candidates.at[idx, 'reasons'].append(f"👤 Sering dipakai ({u_pick}x) (WR {u_wr:.0%})")
                elif u_wr < 0.4 and u_pick >= 2: # Kurang bisa
                    user_score = 20.0
                    candidates.at[idx, 'reasons'].append(f"📉 Riwayat buruk (WR {u_wr:.0%})")
                else: 
                    user_score = 60.0 # Biasa
            
            candidates.at[idx, 'score_user'] = user_score

        # HITUNG TOTAL SKOR AKHIR
        candidates['final_score'] = (
            (candidates['score_external'] * 0.40) + 
            (candidates['score_team']     * 0.30) + 
            (candidates['score_user']     * 0.30)
        )

        # Tambahkan label Meta jika WR tinggi
        for idx in candidates[candidates['win_rate'] > 0.54].index:
            wr = candidates.at[idx, 'win_rate']
            candidates.at[idx, 'reasons'].append(f"🔥 Meta (WR {wr:.1f}%)")

        # Sort & Format Output
        recommendations = candidates.sort_values(by='final_score', ascending=False).head(25)
        
        results = []
        priority_icons = ["✅", "🌟", "👤", "⚔️", "🛡️", "🔥", "⚠️"]
        
        for _, row in recommendations.iterrows():
            unique_reasons = []
            seen = set()
            raw_reasons = row['reasons']
            
            # Sorting alasan agar icon penting muncul duluan
            sorted_reasons = sorted(raw_reasons, key=lambda x: next((i for i, k in enumerate(priority_icons) if k in x), 99))
            
            for r in sorted_reasons:
                if r not in seen:
                    unique_reasons.append(r)
                    seen.add(r)
            
            final_reason = " \n".join(unique_reasons[:3]) 
            results.append({'hero': row['hero_name'], 'reason': final_reason})
            
        return results
    
    def recommend_personalized(self, my_team, enemy_team, banned_heroes, user_profile, username):
        """
        Menghasilkan rekomendasi Terpisah:
        1. User Recs (Top 10) -> Prioritas Comfort Hero & Mastery (Past Experience)
        2. Team Recs (Top 25) -> Fokus ke Meta, Synergy, DAN Exploration (Future Potential)
        """
        # 1. Filter hero yang tersedia
        all_unavailable = (my_team or []) + (enemy_team or []) + (banned_heroes or [])
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable if h])
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        
        if candidates.empty: return [], []

        # 2. Ambil data profil user & NORMALISASI
        raw_preferred = user_profile.get('main_roles', [])
        preferred_roles = [r.lower() for r in raw_preferred]
        
        comfort_heroes = user_profile.get('comfort_heroes', [])
        
        raw_avoid = user_profile.get('avoid_roles', [])
        avoid_roles = [r.lower() for r in raw_avoid]

        current_user = str(username).strip().lower()
        
        # 3. EKSTRAKSI ROLE DARI COMFORT HEROES
        # cari tahu user suka main role spesifik apa dari comfort pick-nya?"
        # misal: comfort hero adalah Ruby (Fighter/Tank). maka user suka fighter dan tank
        comfort_specific_roles = set()
        if comfort_heroes:
            # Ambil data role dari dataframe stats berdasarkan nama hero comfort
            comfort_rows = self.df_stats[self.df_stats['hero_name'].isin(comfort_heroes)]
            for _, c_row in comfort_rows.iterrows():
                # Split role "Fighter/Tank" jadi ["fighter", "tank"]
                # Biar sistem tau user suka main hero tipe apa secara spesifik
                c_roles = str(c_row['role']).lower().replace('/', ',').split(',')
                for r in c_roles:
                    comfort_specific_roles.add(r.strip())

        user_recs = []
        team_recs = []

        for idx, row in candidates.iterrows():
            hero_name = row['hero_name']
            
            # normalisasi nama hero
            role = str(row['role']).lower()
            current_hero_roles = [x.strip() for x in role.replace('/', ',').split(',')]
            
            hero_key = row['join_key']
            is_high_synergy = False
            
            # Stats user
            u_pick, u_wr = self.get_user_hero_stats(hero_name, username)
            
            # --- PHASE A: HITUNG SKOR STRATEGIS (BASE) ---
            meta_score = row['win_rate'] * 100 
            strat_score = meta_score
            strat_reasons = []

            # 1. Meta Status
            if row['win_rate'] > 0.54:
                strat_reasons.append(f"🔥 Meta (WR {row['win_rate']:.1f}%)")
            elif row['win_rate'] > 0.51:
                strat_reasons.append(f"📈 Good Stats (WR {row['win_rate']:.1f}%)")

            # 2. Kebutuhan Tim (Need)
            missing_roles = self.get_team_missing_roles(my_team)
            hero_lane = str(row['lane']).lower()
            is_needed = False
            for needed in missing_roles:
                if needed.split()[0].lower() in hero_lane: 
                    strat_score += 40 
                    strat_reasons.insert(0, f"✅ Isi {needed}")
                    is_needed = True
                    break
            
            # 3. Counter Musuh
            if enemy_team and not self.df_counters.empty:
                for enemy in enemy_team:
                    if not enemy: continue
                    enemy_key = self._normalize_name(enemy)
                    c_row = self.df_counters[
                        (self.df_counters['target_key'] == enemy_key) & 
                        (self.df_counters['counter_key'] == hero_key)
                    ]
                    if not c_row.empty:
                        score_val = float(c_row.iloc[0]['score'])
                        if score_val >= 2.0:
                            strat_score += 25
                            strat_reasons.append(f"⚔️ Hard Counter {enemy}")
                        elif score_val >= 1.0:
                            strat_score += 15
                            strat_reasons.append(f"🛡️ Counter {enemy}")

            # --- [BARU] 4. PERSONALIZED EXPLORATION (Sesuai Role User) ---
            # Jika hero ini sesuai role user TAPI user jarang/belum pernah pakai (u_pick < 5),
            # Berikan boost agar masuk rekomendasi Tim sebagai "Saran Eksplorasi".
            
            # apakah sesuai dengan role utama? (Assassin/Fighter)
            is_user_role = any(r in role for r in preferred_roles)
            
            # apakah mirip dengan comfort pick? (Martis/Arlott)
            is_similar_style = any(r in comfort_specific_roles for r in current_hero_roles)
            
            # apakah user jarang pakai? agar menjadi eksplorasi atau saran
            is_new_experience = (u_pick < 5)
            
            if is_new_experience:
                exploration_boost = 0
                
                # Syarat: Stats Global tidak hancur (>47%)
                if row['win_rate'] > 0.47:
                    
                    # Boost A: Sesuai Main Role (Kita naikkan jadi 25 poin)
                    # Ini biar Assassin/Fighter naik ke atas mengalahkan Tank Meta
                    if is_user_role:
                        exploration_boost += 25
                        strat_reasons.append("✨ Sesuai Role")
                    
                    # Boost B: Mirip Comfort Pick (Kita tambah 15 poin lagi)
                    # Ini biar Paquito/Lancelot makin kuat skornya
                    if is_similar_style:
                        exploration_boost += 15
                        # Cuma tambah teks jika belum ada teks "Sesuai Role" biar ga penuh
                        if not is_user_role: 
                            strat_reasons.append("🎭 Mirip Hero Favorit")
                
                # Masukkan boost ke skor strategis (untuk Tab Kanan)
                strat_score += exploration_boost

            # --- PHASE B: SKOR PERSONAL ---
            user_score = strat_score 
            user_reasons = []

            is_comfort = hero_name in comfort_heroes
            has_history = (u_pick > 0) 
            
            if u_pick > 0:
                if u_wr > 0.6: 
                    user_score += 50
                    user_reasons.insert(0, f"🌟 Hero Andalan")
                elif u_pick >= 3: 
                    user_score += 30
                    user_reasons.append(f"👤 Sering dipakai")
                elif u_wr < 0.4 and u_pick >= 2: 
                    user_score -= 20
                    user_reasons.append(f"📉 Skill issue")
                else: 
                    user_reasons.append(f"📝 History: {u_pick} match")
            
            if is_comfort:
                user_score += 40
                user_reasons.insert(0, "❤️ Comfort Pick")
            
            if is_user_role:
                user_score += 15
                if not user_reasons: user_reasons.append("🎯 Role Utama")

            # --- PHASE C: FINALISASI ---
            combined_user_reasons = user_reasons + strat_reasons[:2] 
            is_avoid = any(r in role for r in avoid_roles)
            valid_for_user = (is_comfort or has_history or is_user_role)
            
            # List USER
            if valid_for_user:
                if is_avoid and not (is_comfort or has_history):
                    pass 
                else:
                    display_reason = " • ".join(combined_user_reasons[:3])
                    if has_history and u_wr < 0.45:
                        display_reason = f"⛔ {display_reason}"

                    user_recs.append({
                        'hero': hero_name,
                        'score': user_score,
                        'wr': u_wr,
                        'pick_count': u_pick,
                        'is_comfort': is_comfort,
                        'has_history': has_history,
                        'reason': display_reason
                    })
            
            # List TEAM (Sinergi Check)
            if not self.df_synergy.empty:
                if 'username' in self.df_synergy.columns:
                    syn_row = self.df_synergy[
                        (self.df_synergy['hero_id'] == hero_key) &
                        (self.df_synergy['username'] == current_user)
                    ]
                else:
                    syn_row = self.df_synergy[self.df_synergy['hero_id'] == hero_key]
                
                if not syn_row.empty:
                    syn_wr = float(syn_row.iloc[0]['synergy_wr'])
                    matches = int(syn_row.iloc[0]['matches_together'])
                    
                    if matches >= 2:
                        if syn_wr > 0.6:
                            strat_score += 35 
                            strat_reasons.insert(0, f"🤝 Sinergi Tinggi")
                            is_high_synergy = True
                        elif syn_wr < 0.4:
                            strat_score -= 15
                            strat_reasons.append(f"⚠️ Bad Synergy")
            
            if strat_score > 60 or is_needed:
                final_strat_reasons = strat_reasons
                
                # Logic text avoid (Updated: Prioritaskan Main Role)
                if is_avoid and not is_user_role:
                    final_strat_reasons.append("⚠️ (Bukan Role Anda)")
                
                team_recs.append({
                    'hero': hero_name,
                    'score': strat_score,
                    'is_high_synergy': is_high_synergy,
                    'reason': " • ".join(final_strat_reasons[:3])
                })

        # --- PHASE D: SORTING ---
        user_recs = sorted(
            user_recs, 
            key=lambda x: (x['is_comfort'], x['has_history'], x['score']), 
            reverse=True
        )[:15]
        
        team_recs = sorted(
            team_recs, 
            key=lambda x: (x['is_high_synergy'], x['score']), 
            reverse=True
        )[:35]
        
        return user_recs, team_recs