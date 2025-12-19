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
   print("--- START SCRAPING 'Hero Statistics' (playwright interceptor) ---")
   
   hero_stats_list = []
   
   with sync_playwright() as p:
      # 1. browser setup
      browser = p.chromium.launch(headless=True)
      page = browser.new_page()

      # 2. interceptor
      def handle_response(response):
         # URL API  dari proses debug
         if "filtered-statistics" in response.url and response.status == 200:
            print("\n--Start to scrape API 'Hero Statistics'")
            try:
               json_data = response.json()
               
               if 'data' in json_data:
                  items = json_data['data']
                  print(f"--Consume payload: {len(items)} hero.")
                  
                  for item in items:
                     # normalize list to string
                     specs = ", ".join(item.get('speciality', []))  

                     # normalize the number
                     win_rate = f"{item.get('win_rate', 0)}%"
                     pick_rate = f"{item.get('pick_rate', 0)}%"
                     ban_rate = f"{item.get('ban_rate', 0)}%"

                     # store to dict
                     hero_stats_list.append({
                        'Hero ID': item.get('hero_id'),
                        'Nama Hero': item.get('hero_name'),
                        'Win Rate': win_rate,
                        'Pick Rate': pick_rate,
                        'Ban Rate': ban_rate,
                        'Speciality': specs,
                        # metadata
                        'Rank Filter': item.get('rank_name', 'ALL'),
                        'Timeframe': item.get('timeframe_name'),
                        'Data Date': item.get('created_at'),
                        'Image URL': item.get('img_src')
                     })
                  else:
                     print("--Valid JSON but the key 'data' not found...")
                     
            except Exception as e:
                  print(f"--Failed to consume and parsing JSON: {e}")

      # 3. pasang penyadap
      page.on("response", handle_response)

      print(f"--\n Open the Target URL '{TARGET_URL}'")
      try:
         # wait_until="networkidle" berarti tunggu sampai tidak ada koneksi jaringan selama 500ms
         page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
         
         time.sleep(5)
      except Exception as e:
         print(f"--Warning for Page Load: {e}")

      browser.close()

   # 5. Simpan ke MinIO
   if hero_stats_list:
      df = pd.DataFrame(hero_stats_list)
      
      print("\n--- Preview Data ---")
      print(df[['Nama Hero', 'Win Rate', 'Pick Rate', 'Rank Filter']].head(3).to_string(index=False))
      
      upload_df_to_minio(df, "mlbb-lakehouse", MINIO_PATH)
      print(f"\n--SUCCESS, {len(df)} data save to MinIO in '{MINIO_PATH}'")
   else:
      print("--FAILED, no data found.")

if __name__ == "__main__":
   scrape_hero_stats()