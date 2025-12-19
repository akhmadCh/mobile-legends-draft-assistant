import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import json
import os

# Setup Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(BASE_DIR, "data", "silver", "training_data.parquet")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def train():
    print("🤖 Memulai Training Model...")
    
    # 1. Cek Data
    if not os.path.exists(DATA_PATH):
        print(f"❌ Error: File data tidak ditemukan di {DATA_PATH}")
        return

    df = pd.read_parquet(DATA_PATH)
    print(f"   Data dimuat: {len(df)} baris.")

    # 2. Siapkan Fitur (X) dan Target (y)
    feature_cols = [c for c in df.columns if c.startswith('T1_') or c.startswith('T2_')]
    X = df[feature_cols]
    y = df['Label_Winner']

    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 4. Train XGBoost
    # Perbaikan: Menghapus 'use_label_encoder' yang sudah deprecated
    model = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        eval_metric='logloss'
    )
    model.fit(X_train, y_train)

    # 5. Evaluasi
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"✅ Training Selesai! Akurasi: {acc:.2%}")

    # 6. Simpan Model & Feature Names
    # PERBAIKAN UTAMA: Menggunakan .get_booster() agar kompatibel dengan XGBoost v3
    model.get_booster().save_model(os.path.join(MODEL_DIR, "draft_model.json"))
    
    with open(os.path.join(MODEL_DIR, "feature_names.json"), "w") as f:
        json.dump(feature_cols, f)
        
    print(f"📂 Model disimpan di: {MODEL_DIR}")

if __name__ == "__main__":
    train()