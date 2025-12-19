from playwright.sync_api import sync_playwright
import json
import time

# URL Target Statistik
TARGET_URL = "https://mlbb.io/hero-statistics"

def debug_stats_structure():
    print("🕵️ Debugging Struktur JSON Statistik MLBB...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def handle_response(response):
            # Kita tangkap semua yang berbau API JSON
            if "api" in response.url and "json" in response.headers.get("content-type", ""):
                try:
                    data = response.json()
                    # Cek ciri-ciri data statistik (biasanya ada key 'data' dan list)
                    if isinstance(data, dict) and 'data' in data:
                        items = data['data']
                        if len(items) > 10: 
                            # Kita cari item pertama yang punya angka statistik (takutnya item[0] kosong)
                            sample_item = items[0]
                            
                            print(f"\n✅ MENEMUKAN PAYLOAD DARI: {response.url}")
                            print("="*50)
                            print("CONTOH DATA STATISTIK MENTAH:")
                            print("="*50)
                            print(json.dumps(sample_item, indent=4))
                            print("="*50)
                except:
                    pass

        page.on("response", handle_response)
        
        print(f"🚀 Membuka {TARGET_URL}...")
        # Statistik biasanya tabel besar, butuh waktu load
        page.goto(TARGET_URL, wait_until="networkidle")
        time.sleep(5) 
        browser.close()

if __name__ == "__main__":
    debug_stats_structure()