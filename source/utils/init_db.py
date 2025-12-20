import pandas as pd
import sqlite3
import os
import random
from source.utils.minio_helper import read_df_from_minio

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# minio path
MINIO_PATH = "raw/temp/hero_master/statistik_hero_raw.csv"

# Nama Database yang akan tercipta
DB_FOLDER = os.path.join(BASE_DIR, "data", "raw", "database")
DB_NAME = f'{DB_FOLDER}/mlbb_internal_sql.db'

def create_mock_sql():
   print("Database SQL Dummy")
   
   try:
      # Baca hasil scraping dari raw/temp
      # df = pd.read_csv(MINIO_PATH)
      df = read_df_from_minio("mlbb-lakehouse", MINIO_PATH)
      
      # filter kolom
      target_columns = ['Nama Hero', 'Win Rate', 'Pick Rate', 'Ban Rate', 'Role', 'Lane', 'Speciality']
      df_sql = df[target_columns].copy()
      
      df_sql.columns = ['hero_name', 'win_rate', 'pick_rate', 'ban_rate', 'role', 'lane', 'speciality']

      # --- SIMPAN KE SQLITE ---
      conn = sqlite3.connect(DB_NAME)
      
      # simpan ke tabel
      df_sql.to_sql('hero_statistics', conn, if_exists='replace', index=False)
      conn.close()
      
      print(f"✅ Database SQL Siap: {DB_NAME}")
      print(f"   Tabel 'hero_statistics' berisi {len(df_sql)} baris.")
      
   except FileNotFoundError:
      print("❌ File 'statistik_hero_raw.csv' di MINIO tidak ditemukan. Jalankan scraping dulu!")

if __name__ == "__main__":
   create_mock_sql()