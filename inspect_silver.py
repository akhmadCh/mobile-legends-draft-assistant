import pandas as pd
import sys, os

# Setup path agar bisa import helper
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from source.utils.minio_helper import read_df_from_minio

BUCKET_NAME = "mlbb-lakehouse"

def check_data_types():
    print("--- 🔍 DIAGNOSA DATA ---")

    # 1. CEK DATA BRONZE (Input)
    print("\n1. Memeriksa File Bronze (Input untuk Silver)...")
    df_bronze = read_df_from_minio(BUCKET_NAME, "bronze/tournament_matches/bronze_mpl_matches.parquet", file_format='parquet')
    
    if df_bronze is not None:
        print(f"   ✅ File ditemukan. Total Match: {len(df_bronze)} baris.")
        
        # Ambil sampel baris pertama
        if len(df_bronze) > 0:
            sample_col = 'left_picks_normalized'
            sample_value = df_bronze[sample_col].iloc[0]
            
            print(f"   👉 Contoh isi kolom '{sample_col}':")
            print(f"       Nilai : {sample_value}")
            print(f"       Tipe  : {type(sample_value)}")  # <--- INI KUNCINYA
            
            if isinstance(sample_value, str):
                print("\n   ⚠️  KESIMPULAN: Data terbaca sebagai STRING. Hipotesis benar.")
                print("       (Kamu PERLU update kode process_silver.py pake 'ast.literal_eval')")
            elif isinstance(sample_value, list):
                print("\n   ✅ KESIMPULAN: Data terbaca sebagai LIST. Kode harusnya aman.")
                print("       (Jika Silver kosong, berarti ada bug lain di logic loop).")
            else:
                print(f"\n   ❓ Tipe data tidak umum: {type(sample_value)}")
    else:
        print("   ❌ File Bronze tidak ditemukan di MinIO.")

    # 2. CEK DATA SILVER (Output Saat Ini)
    print("\n2. Memeriksa File Silver Draft (Output saat ini)...")
    df_silver = read_df_from_minio(BUCKET_NAME, "silver/silver_draft_heroes.parquet", file_format='parquet')
    
    if df_silver is not None:
        print(f"   📊 Total Baris di Silver: {len(df_silver)}")
        if len(df_silver) < 10:
            print("   ❌ SEDIKIT SEKALI! Seharusnya (Jumlah Match x 10).")
            print("   Ini mengonfirmasi bahwa proses explode gagal/ter-skip.")
    else:
        print("   ❌ File Silver tidak ditemukan.")

if __name__ == "__main__":
    check_data_types()