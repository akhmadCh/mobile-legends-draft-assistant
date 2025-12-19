import pandas as pd
from playwright.sync_api import sync_playwright
from source.utils.minio_helper import upload_df_to_minio
import time
import os

# URL Frontend (Kita buka ini, lalu cegat API di belakang layar)
TARGET_URL = "https://mlbb.io/hero-statistics"
# Lokasi simpan di MinIO
MINIO_PATH = "raw/temp/hero_master/statistik_hero_raw.csv"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def scrape_hero_stats():
   print("Scraping Hero Statistics (Playwright Interceptor)...")
   
   hero_stats_list = []
   
   with sync_playwright() as p:
      # 1. Setup Browser
      browser = p.chromium.launch(headless=True)
      page = browser.new_page()

      # 2. Definisikan Interceptor
      def handle_response(response):
         # URL API yang kita temukan dari proses debug
         if "filtered-statistics" in response.url and response.status == 200:
            print("🎯 API Statistik Terdeteksi!")
            try:
               json_data = response.json()
               
               if 'data' in json_data:
                  items = json_data['data']
                  print(f"📦 Mengambil payload: {len(items)} hero.")
                  
                  for item in items:
                     # --- A. Normalisasi List ke String ---
                     specs = ", ".join(item.get('speciality', []))  

                     # --- B. Normalisasi Angka ---
                     # Kita tambahkan "%" agar konsisten dengan format pipeline lama Anda.
                     # Item['win_rate'] adalah float (53.34), kita ubah jadi string "53.34%"
                     win_rate = f"{item.get('win_rate', 0)}%"
                     pick_rate = f"{item.get('pick_rate', 0)}%"
                     ban_rate = f"{item.get('ban_rate', 0)}%"

                     # --- C. Susun Dictionary ---
                     hero_stats_list.append({
                        'Hero ID': item.get('hero_id'),
                        'Nama Hero': item.get('hero_name'),
                        'Win Rate': win_rate,
                        'Pick Rate': pick_rate,
                        'Ban Rate': ban_rate,
                        'Speciality': specs,
                        # Metadata tambahan yang berguna
                        'Rank Filter': item.get('rank_name', 'ALL'),
                        'Timeframe': item.get('timeframe_name'),
                        'Data Date': item.get('created_at'),
                        'Image URL': item.get('img_src')
                     })
                  else:
                     print("⚠️ JSON valid tapi key 'data' tidak ditemukan.")
                     
            except Exception as e:
                  print(f"⚠️ Gagal parsing JSON: {e}")

      # 3. Pasang Penyadapan
      page.on("response", handle_response)

      # 4. Eksekusi
      print(f"🚀 Membuka {TARGET_URL}...")
      try:
         # wait_until="networkidle" berarti tunggu sampai tidak ada koneksi jaringan selama 500ms
         page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
         
         # Tidur sebentar untuk memastikan semua data tabel termuat
         time.sleep(5)
      except Exception as e:
         print(f"⚠️ Peringatan Page Load: {e}")

      browser.close()

   # 5. Simpan ke MinIO
   if hero_stats_list:
      df = pd.DataFrame(hero_stats_list)
      
      print("\n--- Preview Data Statistik ---")
      # Tampilkan kolom utama untuk verifikasi
      print(df[['Nama Hero', 'Win Rate', 'Pick Rate', 'Rank Filter']].head(3).to_string(index=False))
      
      upload_df_to_minio(df, "mlbb-lakehouse", MINIO_PATH)
      print(f"\n✅ [SUKSES] {len(df)} data statistik tersimpan di MinIO.")
   else:
      print("\n❌ [GAGAL] Tidak ada data statistik yang tertangkap.")

if __name__ == "__main__":
   scrape_hero_stats()