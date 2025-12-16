from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# --- SETUP CHROME ---
chrome_options = Options()
# chrome_options.add_argument("--headless") # Kita matikan headless biar kamu bisa lihat browsernya
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

url = "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season"

try:
    print(f"Mengakses {url}...")
    driver.get(url)
    time.sleep(8) # Tunggu loading agak lama

    print("Sedang mencari jejak tim 'RRQ Hoshi'...")
    
    # Cari elemen yang mengandung teks "RRQ Hoshi"
    # Kita pakai XPath untuk mencari teks spesifik
    elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'RRQ Hoshi')]")
    
    if elements:
        print(f"Ditemukan {len(elements)} elemen dengan teks 'RRQ Hoshi'.")
        
        # Ambil elemen pertama yang relevan (biasanya di tabel klasemen atau jadwal)
        # Kita ambil elemen terakhir biasanya yang ada di jadwal match
        target = elements[-1]
        
        # Coba naik ke atas (parent) sampai nemu kotak pembungkus match
        # Kita naik 5 level ke atas untuk melihat strukturnya
        parent = target
        print("\n--- STRUKTUR HTML PEMBUNGKUS ---")
        for i in range(5):
            parent = parent.find_element(By.XPATH, "..") # Naik satu level ke parent
            class_name = parent.get_attribute("class")
            tag_name = parent.tag_name
            print(f"Level Naik {i+1}: Tag=<{tag_name}> | Class='{class_name}'")
            
            # Jika kita menemukan div yang terlihat seperti container match, kita stop dan ambil isinya
            # Ciri-ciri: class mengandung 'row', 'match', 'game', atau 'tr' (table row)
            if class_name and any(x in class_name for x in ['match', 'row', 'list', 'popup']):
                print("\n[BINGO!] Kemungkinan ini container-nya.")
                print("HTML Container:")
                print(parent.get_attribute('outerHTML')[:1000]) # Print 1000 karakter pertama saja
                
                # Simpan ke file biar enak dibaca
                with open("debug_structure.txt", "w", encoding="utf-8") as f:
                    f.write(parent.get_attribute('outerHTML'))
                print("\nFull HTML disimpan ke 'debug_structure.txt'")
                break
    else:
        print("Aneh... Tidak ditemukan teks 'RRQ Hoshi' di halaman ini. Apakah namanya beda?")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()