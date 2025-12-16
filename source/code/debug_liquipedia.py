from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

# --- SETUP CHROME ---
chrome_options = Options()
# Kita matikan headless mode sejenak supaya kamu bisa melihat browsernya bekerja
# chrome_options.add_argument("--headless") 
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

url = "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season"

try:
    print(f"Mengakses {url}...")
    driver.get(url)
    time.sleep(5) # Tunggu loading

    # 1. CARI ELEMENT MATCH
    # Kita cari container utama match
    matches = driver.find_elements(By.CSS_SELECTOR, "div.match-summary")
    
    if len(matches) > 0:
        print(f"Ditemukan {len(matches)} match. Sedang menganalisis match pertama...")
        
        target_match = matches[0]
        
        # 2. COBA KLIK UNTUK MEMBUKA DETAIL (Draft Pick)
        # Kita scroll ke elemen dulu agar bisa diklik
        driver.execute_script("arguments[0].scrollIntoView();", target_match)
        time.sleep(1)
        
        try:
            # Cari tombol toggle di dalam match ini
            toggle_btn = target_match.find_element(By.CSS_SELECTOR, "div.brkts-popup-trigger")
            if toggle_btn:
                print("Klik tombol detail...")
                driver.execute_script("arguments[0].click();", toggle_btn)
                time.sleep(2) # Tunggu popup terbuka
        except:
            print("Tidak menemukan tombol toggle, mencoba ambil HTML mentah saja...")

        # 3. AMBIL HTML DARI MATCH TERSEBUT
        match_html = target_match.get_attribute('outerHTML')
        
        # Simpan ke file teks agar bisa kita baca strukturnya
        with open("debug_match.txt", "w", encoding="utf-8") as f:
            f.write(match_html)
            
        print("\n[SUKSES] Struktur HTML match pertama berhasil disimpan ke 'debug_match.txt'.")
        print("Silakan buka file tersebut, copy isinya, dan kirimkan ke saya.")
        
    else:
        print("[GAGAL] Tidak menemukan elemen class 'match-summary'. Struktur website berubah total.")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()