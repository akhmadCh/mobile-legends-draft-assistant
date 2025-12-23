import pickle
import pandas as pd
import os
import sys

class DraftPredictor:
    def __init__(self):
        # Path relatif dari file ini (source/ml/predictor.py) ke root project
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        
        # Nama file model harus SAMA dengan yang ada di train_model.py
        self.model_path = os.path.join(BASE_DIR, "model_draft_mlbb.pkl")
        
        # Load Model & Feature Names (Sekarang jadi satu paket dalam pickle)
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"--ERROR:Model tidak ditemukan di {self.model_path}. Jalankan source/ml/train_model.py dulu!")

        print(f"Loading model from: {self.model_path}")
        with open(self.model_path, "rb") as f:
            artifact = pickle.load(f)
            self.model = artifact['model']
            self.feature_names = artifact['model_columns']

    def predict_win_rate(self, team_blue_heroes, team_red_heroes):
        """
        Input: List string, e.g. ['Ling', 'Nana'] vs ['Tigreal', 'Layla']
        Output: Float probabilitas kemenangan Tim Biru (0.0 - 1.0)
        """
        # 1. Buat DataFrame kosong dengan kolom yang sesuai urutan training
        #    (XGBoost sangat sensitif terhadap urutan kolom)
        input_data = {col: 0 for col in self.feature_names}
        
        # 2. Isi data Hero Tim Biru (T1)
        for hero in team_blue_heroes:
            # Bersihkan nama hero jika perlu (sesuai logic clean_feature_names di training)
            # Tapi biasanya prefix sudah cukup unik.
            col_name = f"T1_Hero_{hero}"
            # Coba cari exact match dulu
            if col_name in input_data:
                input_data[col_name] = 1
            else:
                # Fallback: Coba match parsial jika ada karakter aneh yang hilang
                # (Opsional, tergantung seberapa bersih data input)
                pass
                
        # 3. Isi data Hero Tim Merah (T2)
        for hero in team_red_heroes:
            col_name = f"T2_Hero_{hero}"
            if col_name in input_data:
                input_data[col_name] = 1
        
        # 4. Convert ke DataFrame (1 baris)
        df_input = pd.DataFrame([input_data])
        
        # Pastikan urutan kolom sama persis dengan self.feature_names
        df_input = df_input[self.feature_names]
        
        # 5. Prediksi Probabilitas
        # classes_[1] adalah probabilitas kelas 1 (Tim Biru Menang)
        try:
            probability = self.model.predict_proba(df_input)[0][1]
        except Exception as e:
            print(f"Error predicting: {e}")
            probability = 0.5 # Default jika error
            
        return probability

# --- Block Test Manual ---
if __name__ == "__main__":
    # Contoh cara pakai langsung
    predictor = DraftPredictor()
    
    # Ganti dengan nama hero yang ada di dataset Anda
    blue = ['Ling', 'Nana', 'Tigreal', 'Layla', 'Saber']
    red  = ['Fanny', 'Gusion', 'Franco', 'Miya', 'Chou']
    
    win_rate = predictor.predict_win_rate(blue, red)
    print(f"\nPrediksi Kemenangan Tim Biru: {win_rate*100:.2f}%")