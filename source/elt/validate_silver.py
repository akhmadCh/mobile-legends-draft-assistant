import pandas as pd
import os

# ==========================================
# KONFIGURASI PATH
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

def validate_data_integrity():
   print("=== MULAI VALIDASI INTEGRITAS DATA SILVER ===\n")

   # 1. Load Semua Dataset Silver
   try:
      df_match = pd.read_parquet(os.path.join(SILVER_DIR, "training_data.parquet"))
      df_stats = pd.read_parquet(os.path.join(SILVER_DIR, "hero_master_clean.parquet"))
      
      # Load Meta dan Counter dari Bronze (karena belum masuk Silver khusus, tapi namanya harus dicek)
      # Atau jika Anda sudah punya di Silver, sesuaikan pathnya. 
      # Asumsi: Kita cek terhadap raw/bronze yang sudah ada karena Meta/Counter biasanya langsung dipakai lookup.
      BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
      df_meta = pd.read_parquet(os.path.join(BRONZE_DIR, "meta_tier.parquet"))
      df_counter = pd.read_parquet(os.path.join(BRONZE_DIR, "counter_hero.parquet"))
      
   except Exception as e:
      print(f"ERROR: Gagal memuat data. Pastikan Ingest & Transform sudah jalan. {e}")
      return

   # 2. Ambil Unique Heroes dari Masing-Masing Sumber
   
   # A. Dari Match (Parse kolom T1_... dan T2_...)
   match_cols = [c for c in df_match.columns if c.startswith('T1_')]
   heroes_in_match = set([c.replace('T1_', '') for c in match_cols])
   print(f"1. Total Hero unik di Data Match (MPL): {len(heroes_in_match)}")
   
   # B. Dari Stats (SQL Master)
   # Pastikan nama kolom sesuai dengan hasil transform Anda ('Nama Hero' atau 'Hero Name')
   heroes_in_stats = set(df_stats['Nama Hero'].unique())
   print(f"2. Total Hero di Master Stats (SQL): {len(heroes_in_stats)}")
   
   # C. Dari Meta
   # Sesuaikan nama kolom di CSV Meta Anda
   meta_col_name = 'Nama Hero' if 'Nama Hero' in df_meta.columns else 'Hero Name' 
   heroes_in_meta = set(df_meta[meta_col_name].unique())
   print(f"3. Total Hero di Data Meta: {len(heroes_in_meta)}")

   # D. Dari Counter
   # Biasanya ada kolom 'Counter_Name' dan 'Target_Name'
   heroes_in_counter = set(df_counter['Counter_Name'].unique()) | set(df_counter['Target_Name'].unique())
   print(f"4. Total Hero di Data Counter: {len(heroes_in_counter)}")

   print("\n--- ANALISIS KESESUAIAN (MATCHING) ---")

   # 3. Cek: Apakah Hero di Match ada di Master Stats?
   missing_in_stats = heroes_in_match - heroes_in_stats
   if missing_in_stats:
      print(f"⚠️  WARNING: Ada {len(missing_in_stats)} hero di Match yang TIDAK ADA di Master Stats!")
      print(f"    Daftar: {missing_in_stats}")
      print("    -> Dampak: Dashboard Winrate hero ini akan kosong.")
   else:
      print("✅ OK: Semua hero Match terdaftar di Master Stats.")

   # 4. Cek: Apakah Hero di Match ada di Meta?
   missing_in_meta = heroes_in_match - heroes_in_meta
   if missing_in_meta:
      print(f"⚠️  WARNING: Ada {len(missing_in_meta)} hero di Match yang TIDAK ADA di Data Meta!")
      print(f"    Daftar: {missing_in_meta}")
      print("    -> Dampak: Hero ini tidak akan punya rekomendasi Tier (dianggap Tier Bawah/Unknown).")
   else:
      print("✅ OK: Semua hero Match punya data Tier Meta.")

   # 5. Cek: Apakah Hero di Match ada di Counter?
   missing_in_counter = heroes_in_match - heroes_in_counter
   if missing_in_counter:
      print(f"⚠️  WARNING: Ada {len(missing_in_counter)} hero di Match yang TIDAK ADA di Data Counter!")
      print(f"    Daftar (Sample 5): {list(missing_in_counter)[:5]}")
      print("    -> Dampak: Sistem tidak tahu siapa counter hero ini.")
   else:
      print("✅ OK: Semua hero Match punya data Counter.")

if __name__ == "__main__":
   print("=== VALIDASI INTEGRITAS DATA SILVER ===")
   validate_data_integrity()
   print("\n=== VALIDASI SELESAI ===")