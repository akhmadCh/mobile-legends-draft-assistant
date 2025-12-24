import pandas as pd
import sys, os
import pickle
import re
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from xgboost import XGBClassifier

# Menambahkan path project ke sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"
MODEL_FILENAME = "model_baseline_draft_mlbb.pkl"

def clean_feature_names(df):
    """Membersihkan nama kolom agar diterima oleh XGBoost (menghapus karakter unik)"""
    new_cols = []
    for col in df.columns:
        # Ganti karakter non-alphanumeric dengan underscore, kecuali jika itu fitur statistik
        clean_col = re.sub(r'[\[\]<>\s]', '_', col)
        new_cols.append(clean_col)
    return new_cols

def train_win_predictor():
    print("--- START TRAINING (XGBOOST + HYPERPARAMETER TUNING) ---")
    
    # 1. Load Data dari Gold Layer
    print("Loading data from MinIO...")
    df = read_df_from_minio(BUCKET_NAME, "gold/features_draft_model.parquet", file_format='parquet')
    if df is None: 
        print("--ERROR: Data not found in Gold Layer.")
        return

    # 2. Fitur Engineering
    target_col = 'Label_Winner'
    
    # A. Ambil Fitur Statistik
    stats_cols = ['T1_Avg_WinRate', 'T1_Total_Counter', 'T1_Avg_Tier',
                'T2_Avg_WinRate', 'T2_Total_Counter', 'T2_Avg_Tier']
    
    # B. Ambil Fitur Nama Hero (One Hot Encoding)
    hero_cols = [c for c in df.columns if 'Hero_' in c]
    
    # Pastikan tipe data hero adalah string/category sebelum get_dummies
    for c in hero_cols:
        df[c] = df[c].astype(str)

    print("Encoding features...")
    X_stats = df[stats_cols]
    X_heroes = pd.get_dummies(df[hero_cols], prefix_sep='_') 
    
    # Gabung Statistik + Nama Hero
    X = pd.concat([X_stats, X_heroes], axis=1)
    y = df[target_col]
    
    # Bersihkan nama kolom untuk XGBoost
    X.columns = clean_feature_names(X)
    
    # Simpan nama kolom untuk referensi saat prediksi nanti
    model_columns = X.columns.tolist()

    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 4. Define Model & Hyperparameters
    print("Configuring XGBoost & Random Search...")
    xgb = XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        use_label_encoder=False,
        random_state=42
    )
    
    # Ruang pencarian parameter (Search Space)
    param_dist = {
        'n_estimators': [100, 200, 300],
        'learning_rate': [0.01, 0.05, 0.1, 0.2],
        'max_depth': [3, 5, 7, 10],
        'subsample': [0.7, 0.8, 1.0],
        'colsample_bytree': [0.7, 0.8, 1.0]
    }
    
    # Mencari parameter terbaik secara otomatis
    random_search = RandomizedSearchCV(
        xgb, 
        param_distributions=param_dist, 
        n_iter=10, # Jumlah kombinasi yang dicoba (tambah jika server kuat)
        scoring='accuracy', 
        cv=3, 
        verbose=1, 
        n_jobs=-1,
        random_state=42
    )
    
    print("Training started (this might take a while)...")
    random_search.fit(X_train, y_train)
    
    # Ambil model terbaik
    best_model = random_search.best_estimator_
    print(f"\n--DONE: Best Params: {random_search.best_params_}")
    
    # 5. Evaluasi
    y_pred = best_model.predict(X_test)
    y_prob = best_model.predict_proba(X_test)[:, 1]
    
    acc = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)
    
    print(f"\n--- HASIL AKHIR ---")
    print(f"Akurasi Model : {acc:.2f} ({acc*100:.1f}%)")
    print(f"ROC-AUC Score : {roc_auc:.3f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 6. Save Model & Columns
    # Kita simpan dictionary agar logic di predictor.py tetap jalan
    artifact = {
        'model': best_model,
        'model_columns': model_columns
    }
    
    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(artifact, f)
    print(f"--SAVED: Model Saved as {MODEL_FILENAME}.")

if __name__ == "__main__":
    train_win_predictor()