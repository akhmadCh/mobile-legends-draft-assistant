import os
import sqlite3
import pandas as pd
from source.utils.minio_helper import upload_df_to_minio

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")
DB_FOLDER = os.path.join(BASE_DIR, "data", "raw", "database")

DB_SOURCE = f'{DB_FOLDER}/mlbb_internal_sql.db'
MINIO_DEST = "raw/internal_db/hero_master_sql.csv"

def ingest_sql_data():
   print("🔌 Extracting from SQL Source...")
   
   try:
      conn = sqlite3.connect(DB_SOURCE)
      
      # Query data dari tabel yang kita buat tadi
      query = "SELECT * FROM hero_statistics"
      df = pd.read_sql(query, conn)
      conn.close()
      
      print(f"Berhasil load {len(df)} row dari SQL.")
      
      # Upload ke MinIO
      upload_df_to_minio(df, "mlbb-lakehouse", MINIO_DEST)
      print("✅ Sukses upload ke MinIO.")
      
   except Exception as e:
      print(f"❌ Error SQL Ingestion: {e}")

if __name__ == "__main__":
   ingest_sql_data()