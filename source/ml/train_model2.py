import pandas as pd
import sys, os
import pickle
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from xgboost import XGBClassifier

# Menambahkan path project ke sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lake"
# Pastikan file ini sesuai dengan output terakhir process_gold2.py
INPUT_FILE = "gold/gold_training_dataset.parquet" 
MODEL_FILENAME = "model_draft_mlbb.pkl"

def train_win_predictor():
    print("--- START TRAINING (STATS BASED PREDICTION) ---")
    
    # 1. Load Data dari Gold Layer (Hasil process_gold2.py)
    print(f"Loading data from {INPUT_FILE}...")
    df = read_df_from_minio(BUCKET_NAME, INPUT_FILE, file_format='parquet')
    
    if df is None: 
        print("--ERROR:Data not found. Pastikan pipeline Gold 2 sudah dijalankan.")
        return
    
    print(f"✅ Data Loaded: {len(df)} matches.")

    # 2. Fitur Engineering (FOKUS: STATISTIK & SELISIH)
    # Kita tidak lagi pakai nama hero (Hero_1, Hero_2) karena bikin overfitting.
    # Kita pakai "Kekuatan Draft" (Stats).
    
    # Target: Apakah Tim Kiri Menang?
    target_col = 'is_winner_team_left'
    
    # Pastikan target integer (0/1) bukan Boolean
    df[target_col] = df[target_col].astype(int)
    
    # Pilih Fitur yang Punya "Sinyal" (Berdasarkan validasi korelasi kita)
    features = [
        # Fitur Utama (Selisih Kekuatan) - Sinyal Terkuat!
        'diff_counter',      # Selisih Counter Score
        'diff_meta',         # Selisih Tier/Meta Score
        'diff_win_rate',     # Selisih Avg Win Rate Hero
        
        # Fitur Tambahan (Statistik Mentah Tim)
        'avg_win_rate_team_left', 'avg_win_rate_team_right',
        'avg_meta_score_team_left', 'avg_meta_score_team_right',
        'avg_counter_score_team_left', 'avg_counter_score_team_right'
    ]
    
    # Cek apakah kolom ada (jaga-jaga jika nama kolom beda dikit)
    available_features = [f for f in features if f in df.columns]
    missing_features = set(features) - set(available_features)
    if missing_features:
        print(f"--WARNING: Kolom berikut tidak ditemukan dan dilewati: {missing_features}")
    
    X = df[available_features]
    y = df[target_col]
    
    print(f"Training dengan {len(available_features)} fitur statistik: {available_features}")
    
    # Simpan nama kolom untuk referensi saat prediksi nanti
    model_columns = X.columns.tolist()

    # 3. Split Data
    # Stratify penting agar proporsi Menang/Kalah seimbang di Train & Test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 4. Define Model & Hyperparameters (XGBoost)
    print("Configuring XGBoost & Random Search...")
    xgb = XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        use_label_encoder=False,
        random_state=42
    )
    
    # Tuning parameter yang sedikit lebih ketat untuk dataset kecil (<1000 row)
    # Agar tidak overfitting
    param_dist = {
        'n_estimators': [50, 100, 200],   # Pohon jangan terlalu banyak
        'learning_rate': [0.01, 0.05, 0.1], 
        'max_depth': [3, 4, 5],           # Depth rendah = lebih general (tidak menghafal)
        'subsample': [0.7, 0.8],
        'colsample_bytree': [0.7, 0.8],
        'gamma': [0, 0.1, 0.2]            # Regularisasi tambahan
    }
    
    random_search = RandomizedSearchCV(
        xgb, 
        param_distributions=param_dist, 
        n_iter=20, 
        scoring='accuracy', 
        cv=5,            # Cross Validation 5-fold biar lebih valid
        verbose=1, 
        n_jobs=-1,
        random_state=42
    )
    
    print("Training started...")
    random_search.fit(X_train, y_train)
    
    # Ambil model terbaik
    best_model = random_search.best_estimator_
    print(f"\n--- SELESAI TRAINING ---")
    print(f"Best Params: {random_search.best_params_}")
    
    # 5. Evaluasi
    y_pred = best_model.predict(X_test)
    y_prob = best_model.predict_proba(X_test)[:, 1]
    
    acc = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)
    
    print(f"\n--- HASIL AKHIR ---")
    print(f"Akurasi Model : {acc:.2f} ({acc*100:.1f}%)")
    print(f"ROC-AUC Score : {roc_auc:.3f}")
    
    # Baseline Check: Apakah lebih baik dari tebak-tebakan (50%)?
    if acc > 0.55:
        print("--DONE: Model lebih baik dari acak (Baseline beat).")
    else:
        print("--DONE BUT: Model masih lemah. Coba tambah data atau fitur.")

    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 6. Save Model & Columns
    artifact = {
        'model': best_model,
        'model_columns': model_columns,
        'features_used': 'stats_only' # Metadata tambahan
    }
    
    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(artifact, f)
    print(f"--SAVED: Model Saved as {MODEL_FILENAME}.")

if __name__ == "__main__":
    train_win_predictor()