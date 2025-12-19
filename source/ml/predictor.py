import xgboost as xgb
import json
import pandas as pd
import numpy as np
import os

class DraftPredictor:
    def __init__(self):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.model_path = os.path.join(BASE_DIR, "models", "draft_model.json")
        self.features_path = os.path.join(BASE_DIR, "models", "feature_names.json")
        
        # Load Model
        self.model = xgb.XGBClassifier()
        self.model.load_model(self.model_path)
        
        # Load Feature Names (Urutan kolom harus persis sama dengan training)
        with open(self.features_path, "r") as f:
            self.feature_names = json.load(f)

    def predict_win_rate(self, team_blue_heroes, team_red_heroes):
        """
        Input: List string, e.g. ['Ling', 'Nana', ...]
        Output: Float probabilitas kemenangan Tim Biru
        """
        # Buat dictionary data kosong dengan nilai 0
        input_data = {col: 0 for col in self.feature_names}
        
        # Isi nilai 1 untuk hero yang dipilih
        for hero in team_blue_heroes:
            col_name = f"T1_{hero}"
            if col_name in input_data:
                input_data[col_name] = 1
                
        for hero in team_red_heroes:
            col_name = f"T2_{hero}"
            if col_name in input_data:
                input_data[col_name] = 1
        
        # Convert ke DataFrame (satu baris)
        df_input = pd.DataFrame([input_data])
        
        # Prediksi (predict_proba mengembalikan [prob_kalah, prob_menang])
        probability = self.model.predict_proba(df_input)[0][1]
        return probability