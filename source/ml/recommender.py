import pandas as pd
import os, sys
import re
import numpy as np

# Setup path agar bisa import helper
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"

class DraftRecommender:
    def __init__(self):
        print("--- [INFO] Initializing Draft Recommender ---")
        
        # 1. Load Data Statistik (Meta)
        self.df_stats = read_df_from_minio(
            BUCKET_NAME, 
            "gold/hero_leaderboard.parquet", 
            file_format='parquet'
        )
        
        # 2. Load Data Counter (Hubungan antar Hero)
        self.df_counters = read_df_from_minio(
            BUCKET_NAME, 
            "gold/hero_counter_lookup.parquet", 
            file_format='parquet'
        )
        
        # 3. Persiapan Data (Cleaning & Normalisasi)
        self._prepare_data()

    def _normalize_name(self, name):
        """
        Membersihkan nama hero agar mudah dicocokkan.
        Contoh: "Yi Sun-shin" -> "yisunshin", "Chang'e" -> "change"
        """
        if pd.isna(name): return ""
        # Hapus semua karakter non-huruf dan jadikan huruf kecil
        clean_name = re.sub(r'[^a-zA-Z]', '', str(name)).lower()
        return clean_name

    def _prepare_data(self):
        """Merapikan nama kolom dan membuat kunci pencarian (join_key)"""
        
        # A. Siapkan Data Statistik
        if self.df_stats is not None and not self.df_stats.empty:
            # Pastikan nama kolom standar agar mudah dipanggil
            # Mapping ini menyesuaikan dengan output parquet gold Anda
            self.df_stats = self.df_stats.rename(columns={
                'hero_name_raw': 'hero_name', # Nama asli untuk ditampilkan
                'win_rate': 'win_rate',
                'ban_rate': 'ban_rate',
                'pick_rate': 'pick_rate',
                'lane': 'lane',
                'role': 'role'
            })
            
            # Buat kolom 'join_key' untuk pencarian yang akurat
            self.df_stats['join_key'] = self.df_stats['hero_name'].apply(self._normalize_name)
            
            # Pastikan angka Win Rate aman (isi 0 jika kosong)
            self.df_stats['win_rate'] = self.df_stats['win_rate'].fillna(0.0).astype(float)
            self.df_stats['ban_rate'] = self.df_stats['ban_rate'].fillna(0.0).astype(float)
        else:
            self.df_stats = pd.DataFrame()

        # B. Siapkan Data Counter
        if self.df_counters is not None and not self.df_counters.empty:
            self.df_counters = self.df_counters.rename(columns={
                'Target_Name': 'target',   # Hero yang dicounter (Korban)
                'Counter_Name': 'counter', # Hero yang mengcounter (Pelaku)
                'Score': 'score'
            })
            
            # Buat key juga di tabel counter
            self.df_counters['target_key'] = self.df_counters['target'].apply(self._normalize_name)
            self.df_counters['counter_key'] = self.df_counters['counter'].apply(self._normalize_name)
        else:
            self.df_counters = pd.DataFrame()

    def get_hero_info(self, hero_name):
        """Mencari info hero berdasarkan nama (input bebas)"""
        if self.df_stats.empty: return None
        
        search_key = self._normalize_name(hero_name)
        # Cari menggunakan join_key agar akurat
        row = self.df_stats[self.df_stats['join_key'] == search_key]
        
        return row.iloc[0] if not row.empty else None

    def get_team_missing_roles(self, current_team):
        """Mengecek role apa yang belum ada di tim kita"""
        filled_lanes = []
        for hero in current_team:
            info = self.get_hero_info(hero)
            if info is not None:
                # Ambil lane utama (biasanya string seperti "Exp Lane, Roam")
                # Kita ambil kata pertamanya saja untuk simplifikasi
                raw_lane = str(info['lane']).lower()
                filled_lanes.append(raw_lane)
        
        filled_string = " ".join(filled_lanes)
        
        # Definisi role standar MLBB
        needed_roles = []
        if "exp" not in filled_string: needed_roles.append("Exp Lane")
        if "gold" not in filled_string: needed_roles.append("Gold Lane")
        if "mid" not in filled_string: needed_roles.append("Mid Lane")
        if "roam" not in filled_string: needed_roles.append("Roamer")
        if "jung" not in filled_string: needed_roles.append("Jungler") # jungle/jungler
        
        return needed_roles

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        """Rekomendasi Ban: Prioritas Meta Tinggi & Counter Tim Kita"""
        if self.df_stats.empty: return []
        
        # Kumpulkan hero yang sudah tidak bisa dipilih
        all_unavailable = my_team + enemy_team + banned_heroes
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable])
        
        # Filter kandidat (yang belum dipick/ban)
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        
        if candidates.empty: return []

        # --- SKORING BAN ---
        # 1. Skor Dasar: Seberapa sering diban (Meta Ban)
        candidates['ban_score'] = candidates['ban_rate'] * 100
        candidates['reasons'] = candidates['ban_rate'].apply(lambda x: [f"⚠️ Sering diban ({x:.1f}%)"])

        # 2. Skor Konteks: Ban hero yang mengancam tim kita
        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                my_key = self._normalize_name(my_hero)
                
                # Cari siapa yang meng-counter hero saya
                threats = self.df_counters[
                    (self.df_counters['target_key'] == my_key) & 
                    (self.df_counters['score'] > 2.0) # Threshold counter keras
                ]
                
                for _, row in threats.iterrows():
                    counter_key = row['counter_key']
                    
                    # Jika counternya ada di kandidat, naikkan skor ban-nya
                    match_idx = candidates.index[candidates['join_key'] == counter_key].tolist()
                    for idx in match_idx:
                        candidates.at[idx, 'ban_score'] += 80 # Prioritas tinggi untuk diban
                        candidates.at[idx, 'reasons'].insert(0, f"🛑 Counter berat {my_hero}")

        # Ambil Top 5
        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(10)
        
        results = []
        for _, row in recommendations.iterrows():
            reason_str = " • ".join(list(dict.fromkeys(row['reasons']))[:2])
            results.append({'hero': row['hero_name'], 'reason': reason_str})
            
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes):
        """Rekomendasi Pick: Seimbang antara Meta, Kebutuhan Tim, dan Counter"""
        if self.df_stats.empty: return []
        
        all_unavailable = my_team + enemy_team + banned_heroes
        unavailable_keys = set([self._normalize_name(h) for h in all_unavailable])
        
        candidates = self.df_stats[~self.df_stats['join_key'].isin(unavailable_keys)].copy()
        
        if candidates.empty: return []

        # Inisialisasi kolom skor dan alasan
        # Skor awal berdasarkan Win Rate (Meta)
        candidates['pick_score'] = candidates['win_rate'] * 100
        candidates['reasons'] = [[] for _ in range(len(candidates))]

        # Tandai hero Meta
        for idx in candidates[candidates['win_rate'] > 0.54].index:
            wr = candidates.at[idx, 'win_rate']
            candidates.at[idx, 'reasons'].append(f"🔥 Meta (WR {wr:.1f}%)")

        # --- 1. CEK KEBUTUHAN ROLE (Sangat Penting) ---
        missing_roles = self.get_team_missing_roles(my_team)
        if missing_roles:
            for idx, row in candidates.iterrows():
                hero_lane = str(row['lane']).lower()
                for role in missing_roles:
                    # Cek kecocokan parsial (misal "gold" ada di "gold lane")
                    keyword = role.split()[0].lower()
                    if keyword in hero_lane:
                        candidates.at[idx, 'pick_score'] += 200 # Tambah poin besar (Wajib isi role)
                        candidates.at[idx, 'reasons'].insert(0, f"✅ Isi {role}")
                        break

        # --- 2. CEK COUNTER (Mengalahkan Musuh) ---
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                enemy_key = self._normalize_name(enemy)
                
                # Cari hero kandidat yang kuat melawan musuh ini
                # (Kita cari di tabel counter dimana target=musuh)
                good_counters = self.df_counters[self.df_counters['target_key'] == enemy_key]
                
                for _, row in good_counters.iterrows():
                    hero_key = row['counter_key'] # Hero kandidat
                    score = float(row['score'])
                    
                    # Cari index kandidat di tabel statistik
                    match_idx = candidates.index[candidates['join_key'] == hero_key].tolist()
                    
                    for idx in match_idx:
                        # Logika Penyeimbang:
                        # WR 50% + Bonus 50 = Skor 100 (Mengalahkan Meta murni WR 60%)
                        
                        if score >= 2.0: # Hard Counter
                            candidates.at[idx, 'pick_score'] += 50 
                            candidates.at[idx, 'reasons'].append(f"⚔️ Hard Counter {enemy}")
                        elif score >= 1.0: # Soft Counter
                            candidates.at[idx, 'pick_score'] += 25
                            candidates.at[idx, 'reasons'].append(f"🛡️ Counter {enemy}")
                        else:
                            candidates.at[idx, 'pick_score'] += 10
                            candidates.at[idx, 'reasons'].append(f"🛡️ Tidak Counter {enemy}")

        # --- 3. CEK SAFETY (Jangan Pick yang Lemah) ---
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                enemy_key = self._normalize_name(enemy)
                
                # Cari apakah kandidat ini lemah lawan musuh?
                # (Kita cari di tabel counter dimana counter=musuh)
                threats = self.df_counters[self.df_counters['counter_key'] == enemy_key]
                
                for _, row in threats.iterrows():
                    victim_key = row['target_key'] # Hero kandidat (korban)
                    
                    match_idx = candidates.index[candidates['join_key'] == victim_key].tolist()
                    for idx in match_idx:
                        candidates.at[idx, 'pick_score'] -= 40 # Hukuman pengurangan poin
                        candidates.at[idx, 'reasons'].append(f"⚠️ Lemah vs {enemy}")

        # --- FINALISASI ---
        # Urutkan berdasarkan skor tertinggi
        recommendations = candidates.sort_values(by='pick_score', ascending=False).head(25)
        
        results = []
        # Urutan prioritas ikon untuk tampilan
        priority_icons = ["✅", "⚔️", "🛡️", "🔥", "⚠️"]
        
        for _, row in recommendations.iterrows():
            # Merapikan list alasan (sort berdasarkan ikon prioritas)
            unique_reasons = []
            seen = set()
            
            raw_reasons = row['reasons']
            # Fungsi sort custom agar ikon penting muncul duluan
            sorted_reasons = sorted(raw_reasons, key=lambda x: next((i for i, k in enumerate(priority_icons) if k in x), 99))
            
            for r in sorted_reasons:
                if r not in seen:
                    unique_reasons.append(r)
                    seen.add(r)
            
            final_reason = " \n".join(unique_reasons[:3]) # Ambil max 3 alasan
            
            results.append({
                'hero': row['hero_name'], 
                'reason': final_reason
            })
            
        return results