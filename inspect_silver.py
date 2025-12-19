import pandas as pd
import os

# Setup Path
BASE_DIR = os.getcwd()
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "training_data.parquet")

def inspect():
    if not os.path.exists(SILVER_PATH):
        print("❌ Data silver belum ada. Jalankan transform_silver.py dulu.")
        return

    # Load Data
    df = pd.read_parquet(SILVER_PATH)
    
    print(f"📊 INFO DATASET")
    print(f"   Total Baris (Match): {len(df)}")
    print(f"   Total Kolom (Fitur): {len(df.columns)}")
    
    # 1. Cek Keseimbangan Label (Menang/Kalah)
    print(f"\n⚖️  SEBARAN MENANG/KALAH:")
    print(df['Label_Winner'].value_counts())
    
    # 2. Cek Sampel Data (Decoding One-Hot)
    print(f"\n🔍 SAMPEL 5 DATA PERTAMA (Isi Fitur):")
    
    # Ambil kolom hero
    t1_cols = [c for c in df.columns if c.startswith('T1_')]
    t2_cols = [c for c in df.columns if c.startswith('T2_')]
    
    for idx, row in df.head(5).iterrows():
        # Cari kolom yang nilainya 1 (Hero yang dipilih)
        t1_heroes = [col.replace('T1_', '') for col in t1_cols if row[col] == 1]
        t2_heroes = [col.replace('T2_', '') for col in t2_cols if row[col] == 1]
        
        winner = "TIM 1 (KIRI)" if row['Label_Winner'] == 1 else "TIM 2 (KANAN)"
        
        print(f"   Match #{idx+1}:")
        print(f"     🔵 Tim 1: {', '.join(t1_heroes)}")
        print(f"     🔴 Tim 2: {', '.join(t2_heroes)}")
        print(f"     🏆 Hasil: {winner}")
        print("-" * 40)

if __name__ == "__main__":
    inspect()