import pandas as pd
import sys, os
import pickle
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"
INPUT_FILE = "gold/gold_training_dataset.parquet" # Pastikan ini output dari pipeline Gold 2 yang baru

def train_baseline_models():
    print("--- 🏁 TRAINING BASELINE MODELS (NB & LOGREG) ---")
    
    # 1. Load Data
    print(f"Loading data from {INPUT_FILE}...")
    df = read_df_from_minio(BUCKET_NAME, INPUT_FILE, file_format='parquet')
    
    if df is None:
        print("❌ Data not found.")
        return

    # 2. Feature Selection (Sama persis dengan XGBoost)
    # Kita pakai fitur statistik yang sudah terbukti valid
    target_col = 'is_winner_team_left'
    df[target_col] = df[target_col].astype(int)
    
    features = [
        'diff_counter',      # Selisih Counter Score
        'diff_meta',         # Selisih Meta Score
        'diff_win_rate',     # Selisih Win Rate
        # Statistik Mentah
        'avg_win_rate_team_left', 'avg_win_rate_team_right',
        'avg_meta_score_team_left', 'avg_meta_score_team_right',
        'avg_counter_score_team_left', 'avg_counter_score_team_right'
    ]
    
    # Filter kolom yang ada saja
    valid_features = [f for f in features if f in df.columns]
    X = df[valid_features]
    y = df[target_col]
    
    print(f"Features used: {len(valid_features)}")

    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # --- MODEL 1: GAUSSIAN NAIVE BAYES ---
    print("\n🤖 Training Gaussian Naive Bayes...")
    nb_model = GaussianNB()
    nb_model.fit(X_train, y_train)
    
    # Evaluasi NB
    nb_pred = nb_model.predict(X_test)
    nb_acc = accuracy_score(y_test, nb_pred)
    print(f"📊 Naive Bayes Accuracy: {nb_acc:.2f} ({nb_acc*100:.1f}%)")

    # --- MODEL 2: LOGISTIC REGRESSION ---
    # LogReg bagus untuk melihat 'Arah' pengaruh fitur (Positif/Negatif)
    print("\n📈 Training Logistic Regression...")
    lr_model = LogisticRegression(random_state=42, max_iter=1000)
    lr_model.fit(X_train, y_train)
    
    # Evaluasi LR
    lr_pred = lr_model.predict(X_test)
    lr_acc = accuracy_score(y_test, lr_pred)
    print(f"📊 Logistic Regression Accuracy: {lr_acc:.2f} ({lr_acc*100:.1f}%)")
    
    # --- INSIGHT: LIHAT KOEFISIEN (Hanya LogReg) ---
    print("\n🔍 Interpretasi Fitur (Logistic Regression):")
    coefs = pd.DataFrame({
        'Feature': valid_features,
        'Weight': lr_model.coef_[0]
    }).sort_values('Weight', ascending=False)
    print(coefs)
    # Weight Positif = Menambah peluang menang
    # Weight Negatif = Mengurangi peluang menang

    # --- SIMPAN MODEL TERBAIK ---
    # Jika Baseline ternyata bagus, kita simpan juga
    best_model = lr_model if lr_acc > nb_acc else nb_model
    model_name = "LogisticRegression" if lr_acc > nb_acc else "NaiveBayes"
    
    artifact = {
        'model': best_model,
        'model_columns': valid_features,
        'model_type': model_name
    }
    
    filename = f"model_baseline_{model_name.lower()}.pkl"
    with open(filename, "wb") as f:
        pickle.dump(artifact, f)
    print(f"\n✅ Saved best baseline model: {filename}")

if __name__ == "__main__":
    train_baseline_models()