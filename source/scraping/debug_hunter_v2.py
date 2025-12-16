from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# --- SETUP CHROME ---
chrome_options = Options()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
url = "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season"

try:
    print(f"Mengakses {url}...")
    driver.get(url)
    time.sleep(8) 

    # 1. Cari semua elemen yang mengandung teks "RRQ Hoshi"
    print("Mencari jejak 'RRQ Hoshi'...")
    elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'RRQ Hoshi')]")
    
    found_valid = False
    
    for i, el in enumerate(elements):
        # Filter elemen sampah (script, style, meta)
        if el.tag_name in ['script', 'style', 'title', 'meta']:
            continue
            
        print(f"\n[JEJAK KE-{i+1}] Tag: <{el.tag_name}> | Text: {el.text[:50]}...")
        
        # 2. Coba naik ke atas (Parent) maksimal 6 level untuk melihat strukturnya
        parent = el
        match_candidate = None
        
        print("   Struktur Pembungkus:")
        for level in range(1, 7):
            try:
                parent = parent.find_element(By.XPATH, "..") # Naik satu level
                class_name = parent.get_attribute("class")
                tag_name = parent.tag_name
                print(f"   Level Up {level}: <{tag_name} class='{class_name}'>")
                
                # Cek apakah ini terlihat seperti kotak match?
                # Ciri-ciri kotak match biasanya div/table dengan class tertentu
                if tag_name == 'div' and class_name and any(x in class_name for x in ['match', 'row', 'game', 'list', 'brkts']):
                    match_candidate = parent
                    print("   >>> BINGO! Ini terlihat seperti container match.")
                    break
            except:
                break # Stop jika sudah mentok di paling atas
        
        # 3. Jika ketemu kandidat yang pas, SIMPAN HTML-nya!
        if match_candidate:
            html_content = match_candidate.get_attribute('outerHTML')
            with open("debug_result_real.txt", "w", encoding="utf-8") as f:
                f.write(html_content)
            print("\n[SUKSES] Struktur HTML asli tersimpan di 'debug_result_real.txt'")
            found_valid = True
            break # Kita cuma butuh satu contoh saja
            
    if not found_valid:
        print("\n[GAGAL] Tidak menemukan struktur match yang jelas. Cek output terminal di atas.")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()