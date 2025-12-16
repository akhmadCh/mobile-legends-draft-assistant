from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib.parse 

# --- SETUP BROWSER ---
chrome_options = Options()
# Biarkan browser terlihat (Headless = False) agar kamu bisa memantau prosesnya
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
url = "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season"

all_match_data = []

try:
    print(f"Mengakses {url}...")
    driver.get(url)
    time.sleep(5) 

    # --- LANGKAH 1: BUKA POPUP MATCH ---
    print("1. Membuka detail pertandingan...")
    toggles = driver.find_elements(By.CSS_SELECTOR, "div.brkts-popup-trigger")
    for toggle in toggles:
        try:
            driver.execute_script("arguments[0].click();", toggle)
            time.sleep(0.05)
        except: pass
    
    print("   Menunggu popup terbuka...")
    time.sleep(3)

    # --- LANGKAH 2: KLIK TOMBOL "SHOW" BANS ---
    print("2. Membuka data Ban (Klik tombol Show)...")
    # Cari tombol Show di dalam tabel mapveto
    show_buttons = driver.find_elements(By.CSS_SELECTOR, ".collapseButtonShow")
    count_clicks = 0
    for btn in show_buttons:
        try:
            if btn.is_displayed():
                driver.execute_script("arguments[0].click();", btn)
                count_clicks += 1
                time.sleep(0.05)
        except: pass
    
    print(f"   Berhasil membuka {count_clicks} tabel bans.")
    print("3. Menunggu gambar hero termuat (10 detik)...")
    time.sleep(10) 

    # --- LANGKAH 3: PARSING DATA (STRUKTUR TERPISAH) ---
    print("4. Membaca HTML...")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    match_containers = soup.find_all('div', class_='brkts-popup')
    
    print(f"Total Match Ditemukan: {len(match_containers)}")

    for i, match in enumerate(match_containers):
        try:
            # --- A. AMBIL NAMA TIM ---
            # Cari semua elemen opponent
            opponents = match.find_all('div', class_='match-info-header-opponent')
            
            team_left = "Unknown"
            team_right = "Unknown"
            winner = "Unknown"

            def get_name(el):
                if not el: return "Unknown"
                link = el.find('a')
                if link and link.get('title'): return link.get('title')
                return el.get_text(strip=True)

            if len(opponents) >= 1:
                team_left = get_name(opponents[0])
                if 'match-info-header-winner' in opponents[0].get('class', []): winner = team_left
            
            if len(opponents) >= 2:
                team_right = get_name(opponents[1])
                if 'match-info-header-winner' in opponents[1].get('class', []): winner = team_right

            # --- B. FUNGSI EKSTRAK HERO ---
            def extract_heroes(container):
                if not container: return []
                heroes = []
                images = container.find_all('img')
                for img in images:
                    name = None
                    if img.get('src'):
                        # Parse dari SRC: .../60px-ML_icon_Cici.png
                        src = img.get('src')
                        filename = src.split('/')[-1]
                        filename = urllib.parse.unquote(filename)
                        name = filename.replace('.png', '').replace('ML_icon_', '')
                        if 'px-' in name: name = name.split('px-')[-1]
                        name = name.replace('_icon', '').replace('_', ' ')
                    
                    if name:
                        clean = name.strip()
                        if len(clean) > 2 and clean not in ['Ban', 'Pick', 'Unknown', 'Show', 'Hide']:
                            heroes.append(clean)
                return list(dict.fromkeys(heroes))

            # --- C. AMBIL PICKS (DARI BODY GAME) ---
            game_picks = []
            pick_divs = match.find_all('div', class_='brkts-popup-body-game')
            
            for p_div in pick_divs:
                # Cari div thumbs kiri dan kanan
                thumbs = p_div.find_all('div', class_='brkts-popup-body-element-thumbs')
                l_p, r_p = [], []
                
                # Biasanya thumbs[0] = Kiri, thumbs[1] = Kanan
                # Cek class 'brkts-popup-body-element-thumbs-right' untuk memastikan
                for t in thumbs:
                    h_list = extract_heroes(t)
                    if 'brkts-popup-body-element-thumbs-right' in t.get('class', []):
                        r_p = h_list
                    else:
                        l_p = h_list
                
                game_picks.append({'left': l_p, 'right': r_p})

            # --- D. AMBIL BANS (DARI TABEL MAPVETO DI BAWAH) ---
            game_bans = []
            veto_table = match.find('div', class_='brkts-popup-mapveto')
            if veto_table:
                # Cari baris yang berisi bans (biasanya ada tr dengan class ban-round)
                ban_rows = veto_table.find_all('tr', class_='brkts-popup-mapveto__ban-round')
                
                for row in ban_rows:
                    # Dalam satu row biasanya ada 3 TD: [Bans Kiri] [Judul Game] [Bans Kanan]
                    tds = row.find_all('td', class_='brkts-popup-mapveto__ban-round-picks')
                    if len(tds) >= 2:
                        l_b = extract_heroes(tds[0])
                        r_b = extract_heroes(tds[1])
                        game_bans.append({'left': l_b, 'right': r_b})
            
            # --- E. GABUNGKAN PICKS & BANS BERDASARKAN URUTAN GAME ---
            # Kita asumsikan urutan Game 1, Game 2 di Picks sama dengan di Bans
            max_games = max(len(game_picks), len(game_bans))
            
            for g_idx in range(max_games):
                # Ambil data picks jika ada
                p_data = game_picks[g_idx] if g_idx < len(game_picks) else {'left': [], 'right': []}
                # Ambil data bans jika ada
                b_data = game_bans[g_idx] if g_idx < len(game_bans) else {'left': [], 'right': []}

                all_match_data.append({
                    'Match_Order': i+1,
                    'Game_Number': g_idx + 1,
                    'Team_Left': team_left,
                    'Team_Right': team_right,
                    'Winner_Match': winner,
                    'Left_Bans': ", ".join(b_data['left']),
                    'Left_Picks': ", ".join(p_data['left']),
                    'Right_Bans': ", ".join(b_data['right']),
                    'Right_Picks': ", ".join(p_data['right'])
                })

        except Exception as e:
            print(f"Error Match {i}: {e}")
            continue

    # 4. SIMPAN CSV
    if all_match_data:
        df = pd.DataFrame(all_match_data)
        file_name = 'data_mpl_s16_split_structure.csv'
        df.to_csv(file_name, index=False)
        print(f"\n[SUKSES] Data tersimpan: {file_name}")
        print(df[['Team_Left', 'Team_Right', 'Left_Bans', 'Left_Picks']].head())
    else:
        print("[GAGAL] Data kosong.")

except Exception as e:
    print(f"Error Utama: {e}")
finally:
    driver.quit()