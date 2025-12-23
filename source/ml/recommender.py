import pandas as pd
import os, sys
import numpy as np

# Setup path agar bisa import helper
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"

class DraftRecommender:
    def __init__(self):
        print("--- [INFO] Initializing Draft Recommender from GOLD Layer ---")
        
        # 1. Load Leaderboard (Data Statistik & Meta)
        # Berisi: hero_name, win_rate, pick_rate, ban_rate, role, lane, tier_score
        self.df_stats = read_df_from_minio(
            BUCKET_NAME, 
            "gold/hero_leaderboard.parquet", 
            file_format='parquet'
        )
        
        # 2. Load Counter Reference (Data Hubungan Counter)
        # Berisi: hero_target, hero_counter, counter_score
        self.df_counters = read_df_from_minio(
            BUCKET_NAME, 
            "gold/hero_counter_lookup.parquet", 
            file_format='parquet'
        )
        
        # 3. Validasi & Mapping Kolom
        # Agar logika di bawah tidak perlu diubah total, kita samakan nama kolomnya
        if self.df_stats is not None and not self.df_stats.empty:
            self.df_stats = self.df_stats.rename(columns={
                'hero_name_raw': 'Nama Hero',
                'win_rate': 'Win Rate',
                'ban_rate': 'Ban Rate',
                'pick_rate': 'Pick Rate',
                'lane': 'Lane',
                'role': 'Role',
                'tier_score': 'Tier'
            })
            # Pastikan tipe data float aman
            self.df_stats['Win Rate'] = self.df_stats['Win Rate'].fillna(0.0).astype(float)
            self.df_stats['Ban Rate'] = self.df_stats['Ban Rate'].fillna(0.0).astype(float)
        else:
            print("⚠️ WARNING: Data Leaderboard Kosong/Gagal Load!")
            self.df_stats = pd.DataFrame()

        if self.df_counters is not None and not self.df_counters.empty:
            self.df_counters = self.df_counters.rename(columns={
                'hero_target': 'Target_Name',
                'hero_counter': 'Counter_Name',
                'counter_score': 'Score'
            })
        else:
            print("⚠️ WARNING: Data Counter Kosong/Gagal Load!")
            self.df_counters = pd.DataFrame()

    def get_hero_info(self, hero_name):
        if self.df_stats.empty: return None
        # Normalisasi nama (antisipasi huruf besar/kecil)
        hero_name = str(hero_name).title() 
        row = self.df_stats[self.df_stats['Nama Hero'] == hero_name]
        return row.iloc[0] if not row.empty else None

    def get_team_missing_roles(self, current_team):
        """Menganalisis role/lane apa yang belum diisi oleh tim."""
        filled_lanes = []
        for hero in current_team:
            info = self.get_hero_info(hero)
            if info is not None and isinstance(info['Lane'], str):
                # Ambil lane utama saja (Gold Layer biasanya sudah bersih, tapi jaga-jaga)
                primary_lane = info['Lane'].split(',')[0].strip()
                filled_lanes.append(primary_lane)
        
        standard_lanes = ['Exp Lane', 'Gold Lane', 'Mid Lane', 'Roam', 'Jungler']
        # Sesuaikan dengan nama lane di dataset Anda (Jungle vs Jungler)
        
        missing = [l for l in standard_lanes if l not in filled_lanes]
        return missing

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        """
        Rekomendasi BAN:
        1. Meta Ban (Hero yang sering diban global)
        2. Contextual Ban (Hero yang meng-counter tim kita)
        """
        if self.df_stats.empty: return []
        
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        
        if candidates.empty: return []

        # --- LOGIC SKOR BAN ---
        
        # 1. Base Score: Meta Ban Rate
        # (Asumsi Ban Rate di Gold adalah float 0.0 - 1.0)
        candidates['ban_score'] = candidates['Ban Rate'] * 100 
        candidates['reasons'] = candidates['Ban Rate'].apply(lambda x: [f"⚠️ Langganan ban! ({x:.2f}%)"])

        # 2. Contextual Ban: Lindungi hero kita
        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                # Cari musuh yang Score counternya tinggi melawan hero kita
                threats = self.df_counters[
                    (self.df_counters['Target_Name'] == my_hero) & 
                    (self.df_counters['Score'] > 2.0) # Threshold counter score (sesuaikan dengan skala data counter)
                ]
                
                for _, row in threats.iterrows():
                    counter_name = row['Counter_Name']
                    if counter_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == counter_name][0]
                        
                        # Tambah skor ban drastis
                        candidates.at[idx, 'ban_score'] += 80 
                        candidates.at[idx, 'reasons'].insert(0, f" Hard Counter untuk {my_hero}")

        # Urutkan dan Format Output
        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(5)
        
        results = []
        for _, row in recommendations.iterrows():
            # Gabungkan alasan (maksimal 2 alasan unik)
            uniq_reasons = list(dict.fromkeys(row['reasons']))[:2]
            reasons_text = " • ".join(uniq_reasons)
            results.append({'hero': row['Nama Hero'], 'reason': reasons_text})
        
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes):
        """
        Rekomendasi PICK Preskriptif:
        1. Win Rate (Meta)
        2. Role Filling (Kebutuhan Tim)
        3. Counter Strategy (Lawan Musuh)
        """
        if self.df_stats.empty: return []
        
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        
        if candidates.empty: return []

        # --- LOGIC SKOR PICK ---

        # 1. BASE SCORE: Win Rate
        candidates['pick_score'] = candidates['Win Rate'] * 100
        candidates['reasons'] = [[] for _ in range(len(candidates))] # Init list kosong

        # Tandai Meta Pick jika WR tinggi (> 54%)
        meta_mask = candidates['Win Rate'] > 0.54
        for idx in candidates[meta_mask].index:
            wr = candidates.at[idx, 'Win Rate']
            candidates.at[idx, 'reasons'].append(f"🔥 Meta Kuat (WR {wr:.2f}%)")

        # 2. ROLE FILLING (Prioritas Utama)
        missing_roles = self.get_team_missing_roles(my_team)
        if missing_roles:
            pattern = '|'.join(missing_roles)
            # Cari hero yang lanenya cocok
            mask = candidates['Lane'].str.contains(pattern, case=False, na=False)
            
            # Boost score besar agar role kosong terisi
            candidates.loc[mask, 'pick_score'] += 200 
            
            for idx in candidates[mask].index:
                lane_data = candidates.at[idx, 'Lane']
                # Cari lane spesifik mana yang cocok
                matched_role = next((r for r in missing_roles if r.lower() in str(lane_data).lower()), lane_data)
                candidates.at[idx, 'reasons'].insert(0, f"Lane  {matched_role}")

        # 3. COUNTER STRATEGY (Keunggulan Taktis)
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                # Cari siapa yang meng-counter musuh ini
                counters = self.df_counters[self.df_counters['Target_Name'] == enemy]
                
                for _, row in counters.iterrows():
                    c_name = row['Counter_Name']
                    score = row['Score']
                    
                    if c_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == c_name][0]
                        
                        # Logika Scoring Counter (Sesuaikan threshold dengan data Anda)
                        # Misal: Score > 1.5 itu counter, > 3.0 itu hard counter
                        if score >= 3.0:
                            candidates.at[idx, 'pick_score'] += 60
                            candidates.at[idx, 'reasons'].append(f"⚔️ Hard Counter {enemy}")
                        elif score >= 1.2:
                            candidates.at[idx, 'pick_score'] += 25
                            candidates.at[idx, 'reasons'].append(f"🛡️ Counter {enemy}")

        # 4. SAFETY CHECK (Jangan pick hero yang sudah dicounter musuh)
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                # Apakah musuh ini adalah counter bagi kandidat kita?
                threats = self.df_counters[self.df_counters['Counter_Name'] == enemy]
                
                for _, row in threats.iterrows():
                    t_name = row['Target_Name'] # Hero kita yang terancam
                    if t_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == t_name][0]
                        
                        # Penalty
                        candidates.at[idx, 'pick_score'] -= 50
                        candidates.at[idx, 'reasons'].append(f"⚠️ Lemah lawan {enemy}")

        # === FINAL FORMATTING ===
        recommendations = candidates.sort_values(by='pick_score', ascending=False).head(5)
        results = []
        
        priority_order = ["✅", "⚔️", "🛡️", "🔥", "⚠️"]
        
        for _, row in recommendations.iterrows():
            raw_reasons = row['reasons']
            # Sort ikon biar rapi
            sorted_reasons = sorted(raw_reasons, key=lambda x: next((i for i, k in enumerate(priority_order) if k in x), 99))
            
            # Hapus duplikat
            unique_reasons = list(dict.fromkeys(sorted_reasons))
            
            final_reason_str = " \n".join(unique_reasons[:3]) # Ambil max 3 poin
            
            results.append({
                'hero': row['Nama Hero'], 
                'reason': final_reason_str
            })
            
        return results