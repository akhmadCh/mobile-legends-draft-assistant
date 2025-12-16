import pandas as pd
import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")
DB_FOLDER = os.path.join(BASE_DIR, "data", "raw", "database")
DB_PATH = f'{DB_FOLDER}/statistik_hero_master.db'

def init_database():
   # cek apakah file csv ada
   if not os.path.exists(CSV_PATH):
      print(f"Error: File {CSV_PATH} tidak ditemukan")
      return
   
   # baca csv
   print("Membaca file scraping CSV 'statistik hero'")
   df = pd.read_csv(CSV_PATH)
   
   # buat folder database jika belum ada
   os.makedirs(DB_FOLDER, exist_ok=True)
   
   # buat koneksi ke SQLite agar membuat file .db otomatis
   conn = sqlite3.connect(DB_PATH)
   
   # masukkan csv ke dalam tabel SQL
   df.to_sql('statistik_hero_master', conn, if_exists='replace', index=False)
   
   # tutup koneksi
   conn.close()
   print(f"SUKSES! Database SQL berhasil dibuat di: {DB_PATH}")
   print("Sekarang sistem kita punya 'Sumber Data SQL' yang valid untuk UAS.")

if __name__ == "__main__":
   init_database()