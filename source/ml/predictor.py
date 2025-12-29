import pickle
import pandas as pd
import numpy as np
import os
import sys

# Path helper
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lake"
MODEL_PATH = os.path.join(BASE_DIR, "model_draft_mlbb.pkl")

GOLD_LEADERBOARD = "gold/hero_leaderboard.parquet"
GOLD_COUNTER = "gold/hero_counter_lookup.parquet"


class DraftPredictor:
    def __init__(self):
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError("Model V3 tidak ditemukan. Jalankan train model dulu.")

        with open(MODEL_PATH, "rb") as f:
            artifact = pickle.load(f)
            self.model = artifact['model']
            self.feature_names = artifact['model_columns']

        # Load Gold Data
        self.hero_stats = read_df_from_minio(BUCKET_NAME, GOLD_LEADERBOARD, file_format="parquet")
        self.counter_lookup = read_df_from_minio(BUCKET_NAME, GOLD_COUNTER, file_format="parquet")

        if self.hero_stats is None or self.counter_lookup is None:
            raise RuntimeError("Gold data tidak lengkap")

    def _normalize(self, name):
        return str(name).lower().replace(" ", "").replace("-", "")

    def _get_hero_row(self, hero):
        key = self._normalize(hero)
        row = self.hero_stats[self.hero_stats['hero_name_normalized'] == key]
        return row.iloc[0] if not row.empty else None

    def _calc_team_stats(self, heroes):
        win_rates = []
        meta_scores = []
        roles = []

        for h in heroes:
            row = self._get_hero_row(h)
            if row is not None:
                win_rates.append(row['win_rate'])
                meta_scores.append(row['tier_score'])
                roles.append(row['role'])

        avg_win_rate = np.mean(win_rates) if win_rates else 0
        avg_meta = np.mean(meta_scores) if meta_scores else 0

        has_front = any(r in ['Tank', 'Fighter', 'Support'] for r in roles)
        has_damage = any(r in ['Marksman', 'Mage', 'Assassin'] for r in roles)
        role_balance = 1 if (has_front and has_damage) else 0

        return avg_win_rate, avg_meta, role_balance

    def _calc_counter_score(self, team, enemy):
        score = 0
        for h in team:
            for e in enemy:
                h_n = self._normalize(h)
                e_n = self._normalize(e)
                row = self.counter_lookup[
                    (self.counter_lookup['Counter_Name'] == h_n) &
                    (self.counter_lookup['Target_Name'] == e_n)
                ]
                if not row.empty:
                    score += row['Score'].iloc[0]
        return score

    def predict_win_rate(self, team_left, team_right):
        # if len(team_left) != 5 or len(team_right) != 5:
        #     raise ValueError("Setiap tim harus terdiri dari 5 hero")

        left_wr, left_meta, left_role = self._calc_team_stats(team_left)
        right_wr, right_meta, right_role = self._calc_team_stats(team_right)

        counter_left = self._calc_counter_score(team_left, team_right)
        counter_right = self._calc_counter_score(team_right, team_left)

        input_data = {
            'diff_team_strength': 0.0,  # default netral
            'diff_counter': counter_left - counter_right,
            'diff_meta': left_meta - right_meta,
            'diff_role_balance': left_role - right_role,
            'diff_win_rate': left_wr - right_wr,
            'avg_meta_score_team_left': left_meta,
            'avg_meta_score_team_right': right_meta,
            'is_role_balanced_left': left_role,
            'is_role_balanced_right': right_role
        }

        df_input = pd.DataFrame([input_data])
        df_input = df_input[self.feature_names]

        prob = self.model.predict_proba(df_input)[0][1]
        return prob


# TEST MANUAL
if __name__ == "__main__":
    predictor = DraftPredictor()

    team_left = ['Ling', 'Nana', 'Tigreal', 'Layla', 'Saber']
    team_right = ['Fanny', 'Gusion', 'Franco', 'Miya', 'Chou']

    prob = predictor.predict(team_left, team_right)
    print(f"Probabilitas Menang Tim Kiri: {prob*100:.2f}%")
