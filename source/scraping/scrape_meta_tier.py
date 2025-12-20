import pandas as pd
from playwright.sync_api import sync_playwright
from source.utils.minio_helper import upload_df_to_minio
import time

# URL Frontend Target
TARGET_URL = "https://mlbb.io/hero-tier"
MINIO_PATH = "raw/hero_meta/meta_tier_raw.csv"

def scrape_meta_tier():
   print("--- START SCRAPING 'Meta' (playwright interceptor) ---")
   
   meta_data_list = []
   
   with sync_playwright() as p:
      browser = p.chromium.launch(headless=True)
      page = browser.new_page()

      def handle_response(response):
         # target API
         if "api/hero/hero-tiers" in response.url and response.status == 200:
            print("\n--Start to scrape API 'Meta'")
            try:
               json_data = response.json()
               if 'data' in json_data:
                  items = json_data['data']
                  print(f"--Consume payload: {len(items)} hero.")
                  
                  for item in items:
                     # Susun Dictionary sesuai struktur JSON yang ditemukan
                     meta_data_list.append({
                        'Hero ID': item.get('hero_id'),
                        'Nama Hero': item.get('hero_name'),
                        'Tier': item.get('tier'),
                        'Previous Tier': item.get('previous_tier'),
                        'Score': item.get('score'), # ranking numerik
                        'Image URL': item.get('img_src') # asset untuk dashboard
                     })
            except Exception as e:
               print(f"--Failed to consume and parsing JSON: {e}")

      page.on("response", handle_response)

      print(f"--\n Open the Target URL {TARGET_URL}")
      try:
         page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
         time.sleep(5)
      except Exception as e:
         print(f"--Warning for Page Load: {e}")
      
      browser.close()

   if meta_data_list:
      df = pd.DataFrame(meta_data_list)
      print("\n--- Preview Data Meta ---")
      print(df[['Nama Hero', 'Tier', 'Score']].head(3).to_string(index=False))
      
      upload_df_to_minio(df, "mlbb-lakehouse", MINIO_PATH)
      print(f"\n--SUCCESS, {len(df)} data save to MinIO in '{MINIO_PATH}'")
   else:
      print("--FAILED, no data found.")

if __name__ == "__main__":
   scrape_meta_tier()