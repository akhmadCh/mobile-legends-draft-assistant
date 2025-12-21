import pandas as pd
import os
import numpy as np

class DraftRecommender:
    def __init__(self):
        # Setup path relatif
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.stats_path = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")
        self.counter_path = os.path.join(BASE_DIR, "data", "raw", "data_counter_mlbb.csv")
        
        # Load Data
        self.df_stats = self._load_stats()
        self.df_counters = self._load_counters()

    def _load_stats(self):
        try:
            if not os.path.exists(self.stats_path): return pd.DataFrame()
            df = pd.read_csv(self.stats_path)
            # Bersihkan persentase
            for col in ['Win Rate', 'Ban Rate', 'Pick Rate']:
                if col in df.columns and df[col].dtype == object:
                    df[col] = df[col].str.rstrip('%').astype('float') / 100.0
            return df
        except Exception as e:
            print(f"Error loading stats: {e}")
            return pd.DataFrame()

    def _load_counters(self):
        try:
            if not os.path.exists(self.counter_path): return pd.DataFrame()
            return pd.read_csv(self.counter_path)
        except Exception as e:
            print(f"Error loading counters: {e}")
            return pd.DataFrame()

    def get_hero_info(self, hero_name):
        if self.df_stats.empty: return None
        row = self.df_stats[self.df_stats['Nama Hero'] == hero_name]
        return row.iloc[0] if not row.empty else None

    def get_team_missing_roles(self, current_team):
        """Menganalisis role apa yang belum diisi oleh tim."""
        filled_lanes = []
        for hero in current_team:
            info = self.get_hero_info(hero)
            if info is not None and isinstance(info['Lane'], str):
                # Ambil lane utama saja (sebelum koma)
                filled_lanes.append(info['Lane'].split(',')[0].strip())
        
        standard_lanes = ['Exp Lane', 'Gold Lane', 'Mid Lane', 'Roam', 'Jungle']
        missing = [l for l in standard_lanes if l not in filled_lanes]
        return missing

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        """
        Memberikan rekomendasi BAN berdasarkan:
        1. Meta Ban Rate (Wajib Ban)
        2. Counter fatal bagi hero yang sudah kita pick.
        """
        if self.df_stats.empty: return []
        
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        
        if candidates.empty: return []

        # 1. Base Score: Seberapa sering hero ini di-ban di publik (Indikator Meta/OP)
        candidates['ban_score'] = candidates['Ban Rate'] * 100 
        candidates['reasons'] = candidates['Ban Rate'].apply(lambda x: [f"⚠️ Sering di-Ban ({x:.1%})"])

        # 2. Contextual Ban: Lindungi hero kita
        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                # Cari hero yang sangat kuat melawan hero kita (Score tinggi)
                threats = self.df_counters[(self.df_counters['Target_Name'] == my_hero) & (self.df_counters['Score'] > 7.0)]
                
                for _, row in threats.iterrows():
                    counter_name = row['Counter_Name']
                    if counter_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == counter_name][0]
                        
                        # Tambah skor ban drastis jika ini hard counter tim kita
                        candidates.at[idx, 'ban_score'] += 80 
                        candidates.at[idx, 'reasons'].insert(0, f"🛑 Hard Counter untuk {my_hero}")

        # Urutkan dan Format Output
        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(5)
        results = []
        for _, row in recommendations.iterrows():
            # Gabungkan alasan menjadi kalimat
            reasons_text = " • ".join(list(dict.fromkeys(row['reasons']))[:2])
            results.append({'hero': row['Nama Hero'], 'reason': reasons_text})
        
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes):
        """
        Memberikan rekomendasi PICK yang Preskriptif:
        - Kenapa hero ini bagus? (Counter / Meta / Synergy)
        - Apa tugasnya? (Isi Role)
        """
        if self.df_stats.empty: return []
        
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        
        if candidates.empty: return []

        # === 1. BASE SCORE (Kekuatan Statistik) ===
        candidates['pick_score'] = candidates['Win Rate'] * 100
        # Inisialisasi list reason
        candidates['reasons'] = [[] for _ in range(len(candidates))]

        # Tandai Meta Pick jika WR tinggi
        meta_mask = candidates['Win Rate'] > 0.54
        for idx in candidates[meta_mask].index:
            wr = candidates.at[idx, 'Win Rate']
            candidates.at[idx, 'reasons'].append(f"🔥 Meta Kuat (WR {wr:.1%})")

        # === 2. ROLE FILLING (Prioritas Utama) ===
        missing_roles = self.get_team_missing_roles(my_team)
        if missing_roles:
            pattern = '|'.join(missing_roles)
            # Cari hero yang lanenya cocok dengan yang kosong
            mask = candidates['Lane'].str.contains(pattern, case=False, na=False)
            
            # Boost score besar agar role kosong terisi dulu
            candidates.loc[mask, 'pick_score'] += 150 
            
            for idx in candidates[mask].index:
                lane_data = candidates.at[idx, 'Lane']
                # Cari lane spesifik mana yang diisi hero ini
                matched_role = next((r for r in missing_roles if r in lane_data), lane_data)
                candidates.at[idx, 'reasons'].insert(0, f"✅ Mengisi {matched_role}")

        # === 3. COUNTER STRATEGY (Keunggulan Taktis) ===
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                # Ambil data siapa yang meng-counter musuh ini
                counters = self.df_counters[self.df_counters['Target_Name'] == enemy]
                
                for _, row in counters.iterrows():
                    c_name = row['Counter_Name']
                    score = row['Score']
                    
                    if c_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == c_name][0]
                        
                        # Penilaian Counter
                        if score >= 8.0:
                            candidates.at[idx, 'pick_score'] += 60 # Boost Besar
                            candidates.at[idx, 'reasons'].append(f"⚔️ Hard Counter {enemy}")
                        elif score >= 5.0:
                            candidates.at[idx, 'pick_score'] += 25 # Boost Sedang
                            candidates.at[idx, 'reasons'].append(f"🛡️ Counter {enemy}")

        # === 4. SAFETY CHECK (Hindari di-counter musuh) ===
        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                # Siapa yang meng-counter hero kandidat ini?
                threats = self.df_counters[self.df_counters['Counter_Name'] == enemy]
                
                for _, row in threats.iterrows():
                    t_name = row['Target_Name']
                    if t_name in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == t_name][0]
                        
                        # Kurangi skor agar tidak merekomendasikan hero yang bakal kalah lane
                        candidates.at[idx, 'pick_score'] -= 40
                        candidates.at[idx, 'reasons'].append(f"⚠️ Lemah lawan {enemy}")

        # === FINAL FORMATTING ===
        recommendations = candidates.sort_values(by='pick_score', ascending=False).head(5)
        results = []
        
        for _, row in recommendations.iterrows():
            # Bersihkan duplikat reason dan urutkan
            unique_reasons = []
            seen = set()
            
            # Prioritas urutan tampilan: Role -> Counter -> Meta -> Warning
            priority_order = ["✅", "⚔️", "🛡️", "🔥", "⚠️"]
            
            # Sort alasan berdasarkan prioritas ikon
            raw_reasons = row['reasons']
            sorted_reasons = sorted(raw_reasons, key=lambda x: next((i for i, k in enumerate(priority_order) if k in x), 99))

            for r in sorted_reasons:
                if r not in seen:
                    unique_reasons.append(r)
                    seen.add(r)
            
            # Gabungkan menjadi string deskriptif
            # Contoh output: "✅ Mengisi Jungler • ⚔️ Hard Counter Fanny • 🔥 Meta Kuat (WR 55%)"
            final_reason_str = " \n".join(unique_reasons[:3]) # Max 3 poin utama
            
            results.append({
                'hero': row['Nama Hero'], 
                'reason': final_reason_str
            })
            
        return results