import time
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from source.utils.minio_helper import upload_df_to_minio

# URL target
URLS = {
   "ID": "https://liquipedia.net/mobilelegends/MPL/Indonesia/Season_16/Regular_Season",
   "PH": "https://liquipedia.net/mobilelegends/MPL/Philippines/Season_16/Regular_Season",
   "MY": "https://liquipedia.net/mobilelegends/MPL/Malaysia/Season_16/Regular_Season",
}

def setup_driver():
   chrome_options = Options()
   chrome_options.add_argument("--disable-gpu")
   chrome_options.add_argument("--start-maximized")
   return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def parse_match_popup(soup, match_order):
   match_data_list = []
   
   # 1. ambil tim kiri, kanan, pemenang
   opponents = soup.find_all('div', class_='match-info-header-opponent')
   team_left = opponents[0].get_text(strip=True) if len(opponents) > 0 else "Unknown"
   team_right = opponents[1].get_text(strip=True) if len(opponents) > 1 else "Unknown"
   
   winner = "Unknown"
   if len(opponents) > 0 and 'match-info-header-winner' in opponents[0].get('class', []): winner = team_left
   elif len(opponents) > 1 and 'match-info-header-winner' in opponents[1].get('class', []): winner = team_right

   # 2. helper ekstrak untuk gambar tiap hero
   def extract_heroes(container):
      if not container: return []
      heroes = []
      for img in container.find_all('img'):
         src = img.get('src', '')
         # Decode nama dari URL file gambar
         fname = urllib.parse.unquote(src.split('/')[-1])
         name = fname.replace('.png', '').replace('ML_icon_', '').replace('_icon', '').replace('_', ' ')
         if 'px-' in name: name = name.split('px-')[-1]
         
         clean_name = name.strip()
         if len(clean_name) > 2 and clean_name not in ['Ban', 'Pick', 'Unknown']:
            heroes.append(clean_name)
      return list(dict.fromkeys(heroes)) # Hapus duplikat

   # 3. ambil informasi picks dan bans
   game_picks = []
   pick_divs = soup.find_all('div', class_='brkts-popup-body-game')
   for p_div in pick_divs:
      thumbs = p_div.find_all('div', class_='brkts-popup-body-element-thumbs')
      l_p, r_p = [], []
      for t in thumbs:
         h = extract_heroes(t)
         if 'brkts-popup-body-element-thumbs-right' in t.get('class', []): r_p = h
         else: l_p = h
      game_picks.append({'left': l_p, 'right': r_p})

   game_bans = []
   veto_table = soup.find('div', class_='brkts-popup-mapveto')
   if veto_table:
      for row in veto_table.find_all('tr', class_='brkts-popup-mapveto__ban-round'):
         tds = row.find_all('td', class_='brkts-popup-mapveto__ban-round-picks')
         if len(tds) >= 2:
            game_bans.append({'left': extract_heroes(tds[0]), 'right': extract_heroes(tds[1])})

   # 4. gabungkan menjadi satu game
   max_games = max(len(game_picks), len(game_bans))
   for g_idx in range(max_games):
      p = game_picks[g_idx] if g_idx < len(game_picks) else {'left': [], 'right': []}
      b = game_bans[g_idx] if g_idx < len(game_bans) else {'left': [], 'right': []}
      
      match_data_list.append({
         'Match_Order': match_order,
         'Game_Number': g_idx + 1,
         'Team_Left': team_left,
         'Team_Right': team_right,
         'Winner_Match': winner,
         'Left_Bans': ", ".join(b['left']),
         'Left_Picks': ", ".join(p['left']),
         'Right_Bans': ", ".join(b['right']),
         'Right_Picks': ", ".join(p['right'])
      })
      
   return match_data_list

def run_scraper():
   driver = setup_driver()
   try:
      for region, url in URLS.items():
         print(f"--- START SCRAPING TOURNAMENT DATA FOR {region} ({url}) ---")
         driver.get(url)
         time.sleep(5)
         
         # A. buka pop up match di website
         toggles = driver.find_elements(By.CSS_SELECTOR, "div.brkts-popup-trigger")
         print(f"   Found {len(toggles)} matches. Opening popups...")
         for toggle in toggles:
            try:
               driver.execute_script("arguments[0].click();", toggle)
               time.sleep(0.1)
            except: pass
         
         # B. buka tombol informasi show bans
         time.sleep(2)
         shows = driver.find_elements(By.CSS_SELECTOR, ".collapseButtonShow")
         for btn in shows:
            try: 
               if btn.is_displayed(): driver.execute_script("arguments[0].click();", btn)
            except: pass
         
         time.sleep(5)
         
         # C. parsing HTML
         full_soup = BeautifulSoup(driver.page_source, 'html.parser')
         popups = full_soup.find_all('div', class_='brkts-popup')
         
         region_data = []
         for i, popup in enumerate(popups):
            match_rows = parse_match_popup(popup, i+1)
            region_data.extend(match_rows)
         
         if region_data:
            df = pd.DataFrame(region_data)
            # minio path, but flexible filename
            filename = f"mpl_{region.lower()}_s16.csv"
            # Simpan ke bucket raw/mpl_matches
            upload_df_to_minio(df, "mlbb-lakehouse", f"raw/mpl_matches/{filename}")
            print(f"\n--SUCCESS, {len(df)} data save to MinIO in 'raw/mpl_matches/{filename}'")
         else:
            print("--FAILED, no data found for {region}.")
   finally:
      driver.quit()

if __name__ == "__main__":
   run_scraper()