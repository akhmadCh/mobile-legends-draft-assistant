import pandas as pd
import os
import numpy as np

class DraftRecommender:
    def __init__(self):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.stats_path = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")
        self.counter_path = os.path.join(BASE_DIR, "data", "raw", "data_counter_mlbb.csv")
        self.df_stats = self._load_stats()
        self.df_counters = self._load_counters()

    def _load_stats(self):
        try:
            if not os.path.exists(self.stats_path): return pd.DataFrame()
            df = pd.read_csv(self.stats_path)
            for col in ['Win Rate', 'Ban Rate', 'Pick Rate']:
                if col in df.columns and df[col].dtype == object:
                    df[col] = df[col].str.rstrip('%').astype('float') / 100.0
            return df
        except: return pd.DataFrame()

    def _load_counters(self):
        try:
            if not os.path.exists(self.counter_path): return pd.DataFrame()
            return pd.read_csv(self.counter_path)
        except: return pd.DataFrame()

    def get_hero_info(self, hero_name):
        if self.df_stats.empty: return None
        row = self.df_stats[self.df_stats['Nama Hero'] == hero_name]
        return row.iloc[0] if not row.empty else None

    def get_team_missing_roles(self, current_team):
        filled_lanes = []
        for hero in current_team:
            info = self.get_hero_info(hero)
            if info is not None and isinstance(info['Lane'], str):
                filled_lanes.append(info['Lane'].split(',')[0].strip())
        standard_lanes = ['Exp Lane', 'Gold Lane', 'Mid Lane', 'Roam', 'Jungle']
        return [l for l in standard_lanes if l not in filled_lanes]

    def recommend_dynamic_ban(self, my_team, enemy_team, banned_heroes):
        if self.df_stats.empty: return []
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        if candidates.empty: return []

        candidates['ban_score'] = candidates['Ban Rate'] * 100 
        candidates['reasons'] = candidates['Ban Rate'].apply(lambda x: [f"Meta Ban ({x:.1%})"])

        if my_team and not self.df_counters.empty:
            for my_hero in my_team:
                threats = self.df_counters[(self.df_counters['Target_Name'] == my_hero) & (self.df_counters['Score'] > 6.0)]
                for _, row in threats.iterrows():
                    if row['Counter_Name'] in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == row['Counter_Name']][0]
                        candidates.at[idx, 'ban_score'] += 50
                        candidates.at[idx, 'reasons'].insert(0, f"Counter {my_hero}")

        recommendations = candidates.sort_values(by='ban_score', ascending=False).head(5)
        results = []
        for _, row in recommendations.iterrows():
            results.append({'hero': row['Nama Hero'], 'reason': ", ".join(list(dict.fromkeys(row['reasons']))[:2])})
        return results

    def recommend_dynamic_pick(self, my_team, enemy_team, banned_heroes):
        if self.df_stats.empty: return []
        unavailable = set(my_team + enemy_team + banned_heroes)
        candidates = self.df_stats[~self.df_stats['Nama Hero'].isin(unavailable)].copy()
        if candidates.empty: return []

        candidates['pick_score'] = candidates['Win Rate'] * 100
        candidates['reasons'] = candidates['Win Rate'].apply(lambda x: [f"WR: {x:.1%}"])

        missing_roles = self.get_team_missing_roles(my_team)
        if missing_roles:
            pattern = '|'.join(missing_roles)
            mask = candidates['Lane'].str.contains(pattern, case=False, na=False)
            candidates.loc[mask, 'pick_score'] += 50
            for idx in candidates[mask].index:
                lane = candidates.at[idx, 'Lane']
                match = next((r for r in missing_roles if r in lane), lane)
                candidates.at[idx, 'reasons'].insert(0, f"Isi {match}")

        if enemy_team and not self.df_counters.empty:
            for enemy in enemy_team:
                counters = self.df_counters[self.df_counters['Target_Name'] == enemy]
                for _, row in counters.iterrows():
                    if row['Counter_Name'] in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == row['Counter_Name']][0]
                        candidates.at[idx, 'pick_score'] += row['Score'] * 3
                        candidates.at[idx, 'reasons'].append(f"Counter {enemy}")
            
            # Safety Check
            for enemy in enemy_team:
                threats = self.df_counters[self.df_counters['Counter_Name'] == enemy]
                for _, row in threats.iterrows():
                    if row['Target_Name'] in candidates['Nama Hero'].values:
                        idx = candidates.index[candidates['Nama Hero'] == row['Target_Name']][0]
                        candidates.at[idx, 'pick_score'] -= 30
                        candidates.at[idx, 'reasons'].append(f"⚠️ Bahaya: Di-counter {enemy}")

        recommendations = candidates.sort_values(by='pick_score', ascending=False).head(5)
        results = []
        for _, row in recommendations.iterrows():
            clean_reasons = list(dict.fromkeys(row['reasons']))
            clean_reasons.sort(key=lambda x: "⚠️" in x, reverse=True)
            results.append({'hero': row['Nama Hero'], 'reason': ", ".join(clean_reasons[:3])})
        return results