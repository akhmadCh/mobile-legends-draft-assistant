import pandas as pd
import sys, os
import pickle
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from xgboost import XGBClassifier

# Path helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lake"
INPUT_FILE = "gold/gold_training_dataset_v3.parquet"  # File output dari process_gold3.py
MODEL_FILENAME = "model_draft_mlbb_v3.pkl"

def train_enhanced_predictor():
    print("--- START TRAINING V3 (ENHANCED FEATURES) ---")
    
    # 1. Load Data
    print(f"Loading data from {INPUT_FILE}...")
    df = read_df_from_minio(BUCKET_NAME, INPUT_FILE, file_format='parquet')
    
    if df is None: 
        print("--ERROR: Data not found. Jalankan process_gold3.py dulu.")
        return
    
    print(f"Data Loaded: {len(df)} matches.")

    # 2. Fitur Engineering
    target_col = 'is_winner_team_left'
    df[target_col] = df[target_col].astype(int)
    
    # Daftar Fitur V3 (Lebih Lengkap)
    features = [
        # --- A. KEKUATAN TIM (Sangat Berpengaruh) ---
        'diff_team_strength',      # Selisih skill historis antar tim
        
        # --- B. KUALITAS DRAFT ---
        'diff_counter',            # Apakah hero kita meng-counter lawan?
        'diff_meta',               # Apakah hero kita lebih meta/OP?
        'diff_role_balance',       # Apakah komposisi role kita lebih lengkap?
        'diff_win_rate',           # Apakah hero kita punya WR tinggi?
        
        # --- C. STATISTIK MENTAH (Opsional, untuk detail) ---
        'avg_meta_score_team_left', 'avg_meta_score_team_right',
        'is_role_balanced_left', 'is_role_balanced_right'
    ]
    
    available_features = [f for f in features if f in df.columns]
    print(f"Features Used ({len(available_features)}): {available_features}")
    
    X = df[available_features]
    y = df[target_col]
    model_columns = X.columns.tolist()

    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 4. XGBoost Setup
    print("Configuring XGBoost...")
    xgb = XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        use_label_encoder=False,
        random_state=42
    )
    
    # Hyperparameter Tuning
    param_dist = {
        'n_estimators': [50, 100, 150],
        'learning_rate': [0.01, 0.05, 0.1], 
        'max_depth': [3, 4, 5],
        'subsample': [0.7, 0.8],
        'colsample_bytree': [0.7, 0.8],
        'gamma': [0, 0.1, 0.5]
    }
    
    random_search = RandomizedSearchCV(
        xgb, 
        param_distributions=param_dist, 
        n_iter=20, 
        scoring='accuracy', 
        cv=5, 
        verbose=1, 
        n_jobs=-1,
        random_state=42
    )
    
    print("Training started...")
    random_search.fit(X_train, y_train)
    best_model = random_search.best_estimator_
    
    # 5. Evaluasi
    y_pred = best_model.predict(X_test)
    y_prob = best_model.predict_proba(X_test)[:, 1]
    
    acc = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)
    
    print(f"\n--- HASIL AKHIR V3 ---")
    print(f"Akurasi Model : {acc:.2f} ({acc*100:.1f}%)")
    print(f"ROC-AUC Score : {roc_auc:.3f}")
    
    print("\nFeature Importance (Top 5):")
    importances = best_model.feature_importances_
    indices = np.argsort(importances)[::-1]
    for i in range(min(5, len(features))):
        print(f"{i+1}. {model_columns[indices[i]]}: {importances[indices[i]]:.4f}")

    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 6. Save Model
    artifact = {
        'model': best_model,
        'model_columns': model_columns,
        'features_used': 'stats_v3'
    }
    
    with open(MODEL_FILENAME, "wb") as f:
        pickle.dump(artifact, f)
    print(f"--SAVED: Model Saved as {MODEL_FILENAME}.")

if __name__ == "__main__":
    train_enhanced_predictor()