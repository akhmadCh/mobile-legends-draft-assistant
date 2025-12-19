import pandas as pd
from playwright.sync_api import sync_playwright
from source.utils.minio_helper import upload_df_to_minio
import time

# URL Frontend Target
TARGET_URL = "https://mlbb.io/hero-tier"
MINIO_PATH = "raw/hero_meta/meta_tier_raw.csv"

def scrape_meta_tier():
   print("Scraping Meta & Tier (Playwright Interceptor)...")
   
   meta_data_list = []
   
   with sync_playwright() as p:
      browser = p.chromium.launch(headless=True)
      page = browser.new_page()

      def handle_response(response):
         # Target API spesifik yang kita temukan tadi
         if "api/hero/hero-tiers" in response.url and response.status == 200:
            print("🎯 API Meta-Tier Terdeteksi!")
            try:
               json_data = response.json()
               if 'data' in json_data:
                  items = json_data['data']
                  print(f"📦 Mengambil payload: {len(items)} hero.")
                  
                  for item in items:
                     # Susun Dictionary sesuai struktur JSON yang ditemukan
                     meta_data_list.append({
                        'Hero ID': item.get('hero_id'),
                        'Nama Hero': item.get('hero_name'),
                        'Tier': item.get('tier'),
                        'Previous Tier': item.get('previous_tier'),
                        'Score': item.get('score'), # Bagus untuk ranking numerik
                        'Image URL': item.get('img_src') # Aset berharga untuk Dashboard
                     })
            except Exception as e:
               print(f"⚠️ Gagal parsing JSON: {e}")

      page.on("response", handle_response)

      print(f"🚀 Membuka {TARGET_URL}...")
      try:
         page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
         time.sleep(5) # Waktu napas tambahan
      except Exception as e:
         print(f"⚠️ Page load warning: {e}")
      
      browser.close()

   # Simpan ke MinIO
   if meta_data_list:
      df = pd.DataFrame(meta_data_list)
      print("\n--- Preview Data Meta ---")
      print(df[['Nama Hero', 'Tier', 'Score']].head(3).to_string(index=False))
      
      upload_df_to_minio(df, "mlbb-lakehouse", MINIO_PATH)
      print(f"\n✅ [SUKSES] {len(df)} data Meta Tier tersimpan.")
   else:
      print("\n❌ [GAGAL] Tidak ada data Meta yang tertangkap.")

if __name__ == "__main__":
   scrape_meta_tier()