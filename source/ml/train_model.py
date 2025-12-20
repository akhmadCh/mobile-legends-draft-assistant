import pandas as pd
import sys, os
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"
MODEL_FILENAME = "model_draft_mlbb.pkl"

def train_win_predictor():
    print("--- 🤖 START TRAINING (RANDOM FOREST + STATS) ---")
    
    # 1. Load Data
    df = read_df_from_minio(BUCKET_NAME, "gold/features_draft_model.parquet", file_format='parquet')
    if df is None: return

    # 2. Fitur Engineering
    target_col = 'Label_Winner'
    
    # A. Ambil Fitur Statistik (Sangat Penting!)
    stats_cols = ['T1_Avg_WinRate', 'T1_Total_Counter', 'T1_Avg_Tier',
                  'T2_Avg_WinRate', 'T2_Total_Counter', 'T2_Avg_Tier']
    
    # B. Ambil Fitur Nama Hero
    hero_cols = [c for c in df.columns if 'Hero_' in c]
    
    # Gabungkan
    X_stats = df[stats_cols]
    X_heroes = pd.get_dummies(df[hero_cols]) # One Hot Encoding Hero
    
    # Gabung Statistik + Nama Hero
    X = pd.concat([X_stats, X_heroes], axis=1)
    y = df[target_col]
    
    # Simpan nama kolom
    model_columns = X.columns.tolist()

    # 3. Split & Train
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("Training Random Forest...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 4. Evaluasi
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    
    print(f"\n--- 🏆 HASIL AKHIR ---")
    print(f"Akurasi Model: {acc:.2f} ({acc*100:.1f}%)")
    print(classification_report(y_test, y_pred))
    
    # 5. Save
    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump({'model': model, 'model_columns': model_columns}, f)
    print("✅ Model Saved.")

if __name__ == "__main__":
    train_win_predictor()