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
GLOBAL_STATS_PATH = "gold/hero_leaderboard.parquet"
COUNTER_DATA_PATH = "gold/hero_counter_lookup.parquet"
HISTORY_FILE_PATH = "gold/user_data/match_history.csv" 

class DraftRecommender:
    def __init__(self):
        print("--- [INFO] Initializing Draft Recommender with Weighted Formula ---")
        
        # 1. Load Data Statistik Global (Meta)
        if MINIO_AVAILABLE:
            self.df_stats = read_df_from_minio(BUCKET_NAME, GLOBAL_STATS_PATH, file_format='parquet')
            self.df_counters = read_df_from_minio(BUCKET_NAME, COUNTER_DATA_PATH, file_format='parquet')
            # 3. Load User History (Data Personal)
            self.df_history = self._load_user_history()
        else:
            # Fallback jika MinIO mati (misal pakai CSV lokal)
            self.df_stats = pd.DataFrame()
            self.df_counters = pd.DataFrame()
            self.df_history = pd.DataFrame(columns=['timestamp', 'my_team', 'enemy_team', 'result'])
        
        # 4. Persiapan Data (Cleaning & Formatting)
        self._prepare_data()

    def _normalize_name(self, name):
        """Membersihkan nama hero untuk pencarian (hapus spasi/simbol, lowercase)."""
        if pd.isna(name) or name is None: return ""
        return re.sub(r'[^a-zA-Z]', '', str(name)).lower()

    # =========================================================================
    # BAGIAN 1: MANAJEMEN HISTORY
    # =========================================================================

    def _load_user_history(self):
        """Membaca data history pertandingan user dari MinIO."""
        print("--- [INFO] Loading User History ---")
        try:
            df = read_df_from_minio(BUCKET_NAME, HISTORY_FILE_PATH, file_format='csv')
            
            if df is None or df.empty:
                print("--- [INFO] No history found. Creating new history log.")
                return pd.DataFrame(columns=['timestamp', 'my_team', 'enemy_team', 'result'])
            
            print(f"--- [INFO] Loaded {len(df)} match history records.")
            return df
        except Exception as e:
            print(f"[WARNING] Error loading history: {e}. Starting fresh.")
            return pd.DataFrame(columns=['timestamp', 'my_team', 'enemy_team', 'result'])

    def save_match_result(self, my_team_list, enemy_team_list, result_status):
        """
        Menyimpan hasil draft pick ke MinIO secara permanen.
        """
        if not MINIO_AVAILABLE:
            print("[ERROR] Cannot save: MinIO not available.")
            return False

        try:
            # 1. Konversi list hero menjadi string
            my_team_clean = [str(x) for x in my_team_list if x]
            enemy_team_clean = [str(x) for x in enemy_team_list if x]

            my_team_str = ",".join(my_team_clean)
            enemy_team_str = ",".join(enemy_team_clean)
            
            # 2. Buat dictionary data baru
            new_data = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'my_team': my_team_str,
                'enemy_team': enemy_team_str,
                'result': result_status
            }
            
            # 3. Update DataFrame di memory
            new_row = pd.DataFrame([new_data])
            self.df_history = pd.concat([self.df_history, new_row], ignore_index=True)
            
            # 4. Upload kembali ke MinIO
            upload_df_to_minio(self.df_history, BUCKET_NAME, HISTORY_FILE_PATH, file_format='csv')
            print(f"[SUCCESS] Match saved to MinIO: {result_status}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to save match result: {e}")
            return False

    def get_user_hero_stats(self, hero_name):
        """
        Menghitung performa user menggunakan hero tertentu berdasarkan history.
        """
        if self.df_history.empty:
            return 0, 0.0
        
        hero_clean = self._normalize_name(hero_name)
        if not hero_clean: return 0, 0.0
        
        # Filter: Cari baris dimana 'my_team' mengandung nama hero
        matches = self.df_history[self.df_history['my_team'].str.lower().str.contains(hero_clean, na=False)]
        
        total_pick = len(matches)
        if total_pick == 0:
            return 0, 0.0
            
        # Hitung kemenangan
        total_win = len(matches[matches['result'] == 'Win'])
        user_wr = total_win / total_pick
        
        return total_pick, user_wr

    # =========================================================================
    # BAGIAN 2: PERSIAPAN DATA
    # =========================================================================

    def _prepare_data(self):
        """Merapikan data global dan counter."""
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
            self.df_stats = self.df_stats.rename(columns={k:v for k,v in rename_map.items() if k in self.df_stats.columns})
            self.df_stats['join_key'] = self.df_stats['hero_name'].apply(self._normalize_name)
            
            if 'win_rate' in self.df_stats.columns:
                self.df_stats['win_rate'] = self.df_stats['win_rate'].fillna(0.0).astype(float)
            if 'ban_rate' in self.df_stats.columns:
                self.df_stats['ban_rate'] = self.df_stats['ban_rate'].fillna(0.0).astype(float)
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
        
        if "exp" not in filled_string: needed_roles.append("Exp Lane")
        if "gold" not in filled_string: needed_roles.append("Gold Lane")
        if "mid" not in filled_string: needed_roles.append("Mid Lane")
        if "roam" not in filled_string: needed_roles.append("Roamer")
        if "jung" not in filled_string: needed_roles.append("Jungler")
        
        return needed_roles

    # =========================================================================
    # BAGIAN 3: ALGORITMA REKOMENDASI (BAN & PICK)
    # =========================================================================

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        """Rekomendasi Ban: Fokus pada Meta & Counter."""
        if self.df_stats.empty: return []
        
        all_unavailable = (my_team or []) + (enemy_team or []) + (banned_heroes or [])
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable if h])
        
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        if candidates.empty: return []

        if 'ban_rate' in candidates.columns:
            candidates['ban_score'] = candidates['ban_rate'] * 100
            candidates['reasons'] = candidates['ban_rate'].apply(lambda x: [f"⚠️ Sering diban ({x:.1f}%)"])
        else:
            candidates['ban_score'] = 0
            candidates['reasons'] = [[] for _ in range(len(candidates))]

        # Logika Counter untuk Ban
        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                if not my_hero: continue
                my_key = self._normalize_name(my_hero)
                threats = self.df_counters[
                    (self.df_counters['target_key'] == my_key) & 
                    (self.df_counters['score'] > 2.0)
                ]
                
                for _, row in threats.iterrows():
                    counter_key = row['counter_key']
                    match_idx = candidates.index[candidates['join_key'] == counter_key].tolist()
                    for idx in match_idx:
                        candidates.at[idx, 'ban_score'] += 80
                        candidates.at[idx, 'reasons'].insert(0, f"🛑 Counter berat {my_hero}")

        # Ambil Top 5
        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(25)
        
        results = []
        for _, row in recommendations.iterrows():
            reason_str = " • ".join(list(dict.fromkeys(row['reasons']))[:2])
            results.append({'hero': row['hero_name'], 'reason': reason_str})
            
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes):
        """
        Rekomendasi Pick dengan Rumus:
        Skor = (Data Eksternal * 40%) + (Kebutuhan Tim * 30%) + (Kecocokan User * 30%)
        """
        if self.df_stats.empty: return []
        
        all_unavailable = (my_team or []) + (enemy_team or []) + (banned_heroes or [])
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable if h])
        
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        
        if candidates.empty: return []

        # Inisialisasi kolom komponen skor
        candidates['score_external'] = 0.0
        candidates['score_team'] = 0.0
        candidates['score_user'] = 0.0
        candidates['reasons'] = [[] for _ in range(len(candidates))]

        # ---------------------------------------------------------------------
        # 1. KOMPONEN DATA EKSTERNAL (40%) - Meta & Counter
        # ---------------------------------------------------------------------
        # Base score dari Win Rate Global (skala 0-100)
        if 'win_rate' in candidates.columns:
            # Win rate biasanya 0.4 - 0.6. Kita boost biar jadi skala 40-60an
            candidates['score_external'] = candidates['win_rate'] * 100
        else:
            candidates['score_external'] = 50.0

        # Tambahkan Logika Counter ke Data Eksternal
        # (Karena counter adalah data objektif eksternal)
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                if not enemy: continue
                enemy_key = self._normalize_name(enemy)
                
                # Cek hero yang meng-counter musuh (Bonus)
                good_counters = self.df_counters[self.df_counters['target_key'] == enemy_key]
                for _, row in good_counters.iterrows():
                    match_idx = candidates.index[candidates['join_key'] == row['counter_key']].tolist()
                    for idx in match_idx:
                        score = float(row['score'])
                        if score >= 2.0:
                            candidates.at[idx, 'score_external'] += 30 # Hard Counter
                            candidates.at[idx, 'reasons'].append(f"⚔️ Hard Counter {enemy}")
                        elif score >= 1.0:
                            candidates.at[idx, 'score_external'] += 15 # Counter
                            candidates.at[idx, 'reasons'].append(f"🛡️ Counter {enemy}")

                # Cek hero yang dilemahkan musuh (Penalty)
                threats = self.df_counters[self.df_counters['counter_key'] == enemy_key]
                for _, row in threats.iterrows():
                     match_idx = candidates.index[candidates['join_key'] == row['target_key']].tolist()
                     for idx in match_idx:
                         candidates.at[idx, 'score_external'] -= 20
                         candidates.at[idx, 'reasons'].append(f"⚠️ Lemah vs {enemy}")

        # ---------------------------------------------------------------------
        # 2. KOMPONEN KEBUTUHAN TIM (30%) - Role Filling
        # ---------------------------------------------------------------------
        # Skala: 100 jika mengisi role yang kosong, 0 jika tidak
        if my_team:
            missing_roles = self.get_team_missing_roles(my_team)
            if missing_roles:
                for idx, row in candidates.iterrows():
                    if 'lane' in row:
                        hero_lane = str(row['lane']).lower()
                        for role in missing_roles:
                            keyword = role.split()[0].lower() # e.g. "gold" from "Gold Lane"
                            if keyword in hero_lane:
                                candidates.at[idx, 'score_team'] = 100.0
                                candidates.at[idx, 'reasons'].insert(0, f"✅ Isi {role}")
                                break
        else:
            # Jika belum ada pick (first pick), semua role dibutuhkan
            candidates['score_team'] = 100.0

        # ---------------------------------------------------------------------
        # 3. KOMPONEN KECOCOKAN USER (30%) - History
        # ---------------------------------------------------------------------
        # Skala: 0 - 100 berdasarkan performa user
        for idx, row in candidates.iterrows():
            hero_name = row['hero_name']
            u_pick, u_wr = self.get_user_hero_stats(hero_name)
            
            user_score = 50.0 # Nilai netral jika belum pernah pakai
            
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
                else: # Biasa aja
                    user_score = 60.0
            
            candidates.at[idx, 'score_user'] = user_score

        # ---------------------------------------------------------------------
        # HITUNG TOTAL SKOR AKHIR BERDASARKAN RUMUS
        # Skor = (External * 0.4) + (Team * 0.3) + (User * 0.3)
        # ---------------------------------------------------------------------
        candidates['final_score'] = (
            (candidates['score_external'] * 0.40) + 
            (candidates['score_team']     * 0.30) + 
            (candidates['score_user']     * 0.30)
        )

        # Meta info untuk display alasan jika Win Rate tinggi
        for idx in candidates[candidates['win_rate'] > 0.54].index:
            wr = candidates.at[idx, 'win_rate']
            candidates.at[idx, 'reasons'].append(f"🔥 Meta (WR {wr:.1f}%)")

        # Sort berdasarkan Final Score
        recommendations = candidates.sort_values(by='final_score', ascending=False).head(25)
        
        results = []
        priority_icons = ["✅", "🌟", "👤", "⚔️", "🛡️", "🔥", "⚠️"]
        
        for _, row in recommendations.iterrows():
            unique_reasons = []
            seen = set()
            raw_reasons = row['reasons']
            
            # Urutkan alasan biar icon penting muncul duluan
            sorted_reasons = sorted(raw_reasons, key=lambda x: next((i for i, k in enumerate(priority_icons) if k in x), 99))
            
            for r in sorted_reasons:
                if r not in seen:
                    unique_reasons.append(r)
                    seen.add(r)
            
            final_reason = " \n".join(unique_reasons[:3]) 
            results.append({'hero': row['hero_name'], 'reason': final_reason})
            
        return results