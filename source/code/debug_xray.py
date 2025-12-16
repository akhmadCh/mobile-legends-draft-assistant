from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# --- SETUP CHROME ---
chrome_options = Options()
# Headless FALSE supaya kamu bisa melihat browsernya terbuka dan nge-klik sendiri
# chrome_options.add_argument("--headless") 
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

url = "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season"

try:
    print(f"Mengakses {url}...")
    driver.get(url)
    time.sleep(5) # Tunggu loading

    # 1. AMBIL SATU MATCH SAJA
    print("Mencari elemen 'match-summary'...")
    matches = driver.find_elements(By.CSS_SELECTOR, "div.match-summary")
    
    if len(matches) > 0:
        target = matches[0] # Kita bedah match pertama saja
        print("Match ditemukan! Mencoba membuka detail...")

        # Scroll ke elemen biar kelihatan
        driver.execute_script("arguments[0].scrollIntoView();", target)
        time.sleep(1)

        # 2. CARI TOMBOL KLIK (Biasanya class 'brkts-popup-trigger')
        try:
            # Cari elemen apapun di dalam match ini yang bisa diklik untuk expand
            # Di Liquipedia biasanya ada di bagian footer kotak match
            toggle = target.find_element(By.CSS_SELECTOR, "div.brkts-popup-trigger")
            if toggle:
                print("Klik tombol toggle...")
                driver.execute_script("arguments[0].click();", toggle)
                time.sleep(3) # Tunggu animasi popup keluar
        except Exception as e:
            print(f"Gagal klik toggle (mungkin sudah terbuka atau class beda): {e}")

        # 3. AMBIL HTML LENGKAP
        # Kita ambil outerHTML dari target match ini
        html_content = target.get_attribute('outerHTML')

        # Simpan ke file
        filename = "hasil_bedah_html.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print("\n" + "="*50)
        print(f"[BERHASIL] HTML disimpan ke file '{filename}'")
        print("="*50)
        print("TUGAS KAMU: Buka file tersebut, copy semua isinya, dan paste di chat Gemini.")
        
    else:
        print("Aneh.. Elemen 'match-summary' tidak ditemukan (padahal sebelumnya ada).")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()