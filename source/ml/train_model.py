import pandas as pd
import pickle
import json
import os
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(BASE_DIR, "data", "silver", "training_data.parquet")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def train():
    print("🤖 Training Model Final (BernoulliNB - Full Features)...")
    
    if not os.path.exists(DATA_PATH):
        print("❌ Data tidak ditemukan.")
        return

    df = pd.read_parquet(DATA_PATH)
    
    # 1. Gunakan SEMUA fitur Hero (Tanpa membuang yang jarang muncul)
    feature_cols = [c for c in df.columns if c.startswith('T1_') or c.startswith('T2_')]
    X = df[feature_cols]
    y = df['Label_Winner']
    
    print(f"   📊 Total Data: {len(df)} match")
    print(f"   📊 Total Fitur: {len(feature_cols)} hero")

    # 2. Split Data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # 3. Train BernoulliNB
    # alpha=1.0 adalah standar smoothing agar tidak terlalu overfitting
    model = BernoulliNB(alpha=1.0) 
    model.fit(X_train, y_train)

    # 4. Evaluasi
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    
    print(f"\n✅ Akurasi Test Set: {acc:.2%}")
    print("\nLaporan Klasifikasi:")
    print(classification_report(y_test, preds))
    
    print("Matriks Kebingungan:")
    print(confusion_matrix(y_test, preds))

    # 5. Simpan Model & Fitur
    with open(os.path.join(MODEL_DIR, "draft_model_nb.pkl"), "wb") as f:
        pickle.dump(model, f)
    
    # Simpan semua kolom fitur agar predictor tidak error
    with open(os.path.join(MODEL_DIR, "feature_names.json"), "w") as f:
        json.dump(feature_cols, f)
        
    print(f"📂 Model disimpan di: {MODEL_DIR}")

if __name__ == "__main__":
    train()