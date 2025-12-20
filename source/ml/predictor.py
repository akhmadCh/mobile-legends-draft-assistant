import pickle
import json
import pandas as pd
import os

class DraftPredictor:
    def __init__(self):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        # Perhatikan nama file modelnya beda (.pkl)
        self.model_path = os.path.join(BASE_DIR, "models", "draft_model_nb.pkl")
        self.features_path = os.path.join(BASE_DIR, "models", "feature_names.json")
        
        # Load Model (Pickle)
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"Model tidak ditemukan di {self.model_path}. Jalankan train_model.py dulu!")

        with open(self.model_path, "rb") as f:
            self.model = pickle.load(f)
        
        # Load Feature Names
        with open(self.features_path, "r") as f:
            self.feature_names = json.load(f)

    def predict_win_rate(self, team_blue_heroes, team_red_heroes):
        """
        Input: List string, e.g. ['Ling', 'Nana', ...]
        Output: Float probabilitas kemenangan Tim Biru
        """
        # Buat data input kosong (0 semua)
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
        
        # Convert ke DataFrame
        df_input = pd.DataFrame([input_data])
        
        # Prediksi Probabilitas
        # classes_[1] biasanya adalah label 1 (Menang)
        probability = self.model.predict_proba(df_input)[0][1]
        return probability