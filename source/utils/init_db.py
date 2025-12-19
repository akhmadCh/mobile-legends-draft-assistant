import pandas as pd
import sqlite3
import os
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")
DB_FOLDER = os.path.join(BASE_DIR, "data", "raw", "database")

# folder asal (raw)
DATA_DIR = os.path.join(BASE_DIR, "data")

# File CSV hasil scraping lokal tadi
INPUT_CSV = os.path.join(DATA_DIR, "temp", "temp_hero_data_for_setup.csv")

# Nama Database yang akan tercipta
DB_NAME = f'{DB_FOLDER}/mlbb_internal_sql.db'

def create_mock_sql():
   print("Database SQL Dummy")
   
   try:
      # Baca hasil scraping
      df = pd.read_csv(INPUT_CSV)
      
      # --- PERKAYA DATA (Agar beda dengan scraping) ---
      # Kita tambahkan kolom harga seolah-olah ini database toko
      print("   Menambahkan data harga dummy...")
      
      # Harga BP biasanya: 32000, 24000, 15000, 6500
      bp_options = [32000, 24000, 15000, 6500]
      df['battle_point_cost'] = [random.choice(bp_options) for _ in range(len(df))]
      
      # Harga Diamond biasanya: 599, 499, 399, 299
      dm_options = [599, 499, 399, 299]
      df['diamond_cost'] = [random.choice(dm_options) for _ in range(len(df))]
      
      # Kita HAPUS kolom statistik (WinRate dll) di versi SQL ini
      # Agar SQL murni berisi "Data Master Hero" (Nama, Role, Harga)
      # Jadi nanti di Silver Layer kita JOIN: SQL (Harga) + Scraper (Winrate)
      cols_to_keep = ['Nama Hero', 'Role', 'Lane', 'Speciality', 'battle_point_cost', 'diamond_cost']
      # Pastikan kolom ada sebelum filter
      existing_cols = [c for c in cols_to_keep if c in df.columns]
      df_sql = df[existing_cols]

      # --- SIMPAN KE SQLITE ---
      conn = sqlite3.connect(DB_NAME)
      df_sql.to_sql('master_hero_price', conn, if_exists='replace', index=False)
      conn.close()
      
      print(f"✅ Database SQL Siap: {DB_NAME}")
      print(f"   Tabel 'master_hero_price' berisi {len(df_sql)} baris.")
      
   except FileNotFoundError:
      print("❌ File CSV lokal tidak ditemukan. Jalankan scraping dulu!")

if __name__ == "__main__":
   create_mock_sql()