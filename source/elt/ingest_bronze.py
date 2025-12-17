import pandas as pd
import sqlite3
import os
import glob

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# folder asal (Raw)
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DB_PATH = os.path.join(RAW_DIR, "database", "statistik_hero_master.db")

# folder tujuan (Bronze)
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

# Pastikan folder Bronze ada, jika tidak, buat folder baru
os.makedirs(BRONZE_DIR, exist_ok=True)

def ingest_hero_stats_sql():
   """
   Sumber 1: Database SQL (SQLite) -> Bronze Parquet
   """
   print(f"[INGEST] Membaca Database SQL dari {DB_PATH}...")
   
   if not os.path.exists(DB_PATH):
      print(f"ERROR: Database tidak ditemukan di {DB_PATH}")
      return

   try:
      conn = sqlite3.connect(DB_PATH)
      # Baca semua data dari tabel statistik_hero_master
      df = pd.read_sql("SELECT * FROM statistik_hero_master", conn)
      conn.close()
      
      # Simpan ke Bronze
      output_path = os.path.join(BRONZE_DIR, "hero_stats.parquet")
      df.to_parquet(output_path, index=False)
      print(f"SUCCESS: Data Hero Stats tersimpan di {output_path} ({len(df)} baris)")
      
   except Exception as e:
      print(f"ERROR saat ingest SQL: {e}")

def ingest_match_data():
   """
   Sumber 2: CSV Data Pertandingan MPL ID & PH S16 -> Bronze Parquet
   """
   target_files = {
      "ID": "data_mpl_id_s16.csv", # Pastikan file ini ada di folder raw
      "PH": "data_mpl_ph_s16.csv"
   }
   
   # Mapping Nama Turnamen berdasarkan Region
   tournament_map = {
      "ID": "MPL ID S16",
      "PH": "MPL PH S16"
   }
   
   dfs = []
   print("[INGEST] Memulai penggabungan data Match MPL (ID & PH)...")
   
   for region, filename in target_files.items():
      input_path = os.path.join(RAW_DIR, filename)
      
      if os.path.exists(input_path):
         try:
            print(f"   -> Membaca {filename} ({region})...")
            df_temp = pd.read_csv(input_path)
            
            # kolom region dan tournament
            df_temp['region'] = region 
            df_temp['tournament'] = tournament_map.get(region, f"Unknown ({region})")
            
            dfs.append(df_temp)
         except Exception as e:
            print(f"   ERROR membaca {filename}: {e}")
      else:
         print(f"   WARNING: File {filename} tidak ditemukan di Raw Layer.")
   
   if dfs:
      # gabungkan semua dataframe jadi satu
      full_df = pd.concat(dfs, ignore_index=True)
      
      output_path = os.path.join(BRONZE_DIR, "mpl_matches.parquet")
      full_df.to_parquet(output_path, index=False)
      print(f"SUCCESS: Gabungan Data MPL (ID & PH) tersimpan di {output_path} ({len(full_df)} baris)")
   else:
      print("ERROR: Tidak ada data MPL yang berhasil dibaca.")

def ingest_meta_tier():
   """
   Sumber 3: CSV Meta (Multi-files SS, A, B, etc) -> Bronze Parquet
   Menggabungkan semua file Tier menjadi satu dataset.
   """
   print("[INGEST] Membaca Data Meta (Tier List)...")
   
   # cari semua file yang pola namanya 'data_hero_mlbb_tier_*.csv'
   pattern = os.path.join(RAW_DIR, "data_hero_mlbb_tier_*.csv")
   files = glob.glob(pattern)
   
   if not files:
      print("WARNING: Tidak ada file Meta Tier ditemukan.")
      return

   print(f"   -> Ditemukan {len(files)} file tier: {[os.path.basename(f) for f in files]}")

   dfs = []
   for file in files:
      try:
         temp_df = pd.read_csv(file)
         dfs.append(temp_df)
      except Exception as e:
         print(f"Error membaca file {file}: {e}")
   
   if dfs:
      # gabungkan (Concat) semua dataframe
      full_df = pd.concat(dfs, ignore_index=True)
      
      # simpan ke Bronze
      output_path = os.path.join(BRONZE_DIR, "meta_tier.parquet")
      full_df.to_parquet(output_path, index=False)
      print(f"SUCCESS: Data Meta Tier tersimpan di {output_path} ({len(full_df)} baris)")

def ingest_counter_data():
   """
   Sumber 4: CSV Counter Hero -> Bronze Parquet
   """
   csv_file = "data_counter_mlbb.csv"
   input_path = os.path.join(RAW_DIR, csv_file)
   
   print(f"[INGEST] Membaca Data Counter dari {input_path}...")
   
   if os.path.exists(input_path):
      df = pd.read_csv(input_path)
      
      # Simpan ke Bronze
      output_path = os.path.join(BRONZE_DIR, "counter_hero.parquet")
      df.to_parquet(output_path, index=False)
      print(f"SUCCESS: Data Counter tersimpan di {output_path} ({len(df)} baris)")
   else:
      print(f"WARNING: File {csv_file} tidak ditemukan.")

if __name__ == "__main__":
   print("=== MULAI PROSES INGESTION (RAW -> BRONZE) ===")
   ingest_hero_stats_sql()
   ingest_match_data()
   ingest_meta_tier()
   ingest_counter_data()
   print("=== INGESTION SELESAI ===")