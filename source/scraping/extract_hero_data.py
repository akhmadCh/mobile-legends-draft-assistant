import pandas as pd
from bs4 import BeautifulSoup
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def extract_hero_data_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # --- 1. OTOMATIS CARI NAMA TIER ---
    # Mencari div header tier berdasarkan struktur yang kamu berikan
    tier_header_div = soup.find('div', class_='flex flex-col items-center')
    
    tier_name = 'Unknown'
    if tier_header_div:
        # Mencari span yang berisi teks besar (SS, S, A, dll)
        # Kita gunakan partial match pada class 'text-3xl' agar aman
        tier_span = tier_header_div.find('span', class_=lambda x: x and 'text-3xl' in x)
        if tier_span:
            tier_name = tier_span.get_text(strip=True)
    
    print(f"Tier Terdeteksi: {tier_name}")

    # --- 2. CARI HERO CARDS ---
    hero_list = []
    
    # Mencari semua elemen <a> dengan class spesifik hero card
    # Menggunakan sebagian class utama agar tetap fleksibel namun akurat
    hero_cards = soup.find_all('a', class_=lambda x: x and 'group relative flex flex-col items-center' in x)
    
    if not hero_cards:
        print("Tidak ada hero ditemukan. Pastikan HTML disalin dengan benar.")
        return []

    for card in hero_cards:
        # A. Ambil Nama Hero (di dalam tag h3)
        name_tag = card.find('h3', class_='font-semibold text-xs text-center leading-tight')
        hero_name = name_tag.get_text(strip=True) if name_tag else 'N/A'
        
        # B. Ambil Role dan Lane dari Tooltip (div invisible)
        tooltip = card.find('div', class_=lambda x: x and 'absolute invisible group-hover:visible' in x)
        
        hero_role = 'N/A'
        hero_lane = 'N/A'
        
        if tooltip:
            # Loop setiap paragraf <p> di dalam tooltip
            info_paragraphs = tooltip.find_all('p')
            for p in info_paragraphs:
                full_text = p.get_text(strip=True) # Hasil contoh: "Roles:Tank"
                
                if 'Roles:' in full_text:
                    hero_role = full_text.replace('Roles:', '').strip()
                elif 'Lanes:' in full_text:
                    hero_lane = full_text.replace('Lanes:', '').strip()
        
        # Masukkan ke list
        hero_list.append({
            'Tier': tier_name,
            'Nama Hero': hero_name,
            'Role': hero_role,
            'Lane': hero_lane
        })
        
    return hero_list

# --- BAGIAN EKSEKUSI ---

input_file = 'input_semua_tier.html'
# output_file = 'data_tier_atau_meta_hero.csv'
output_file = os.path.join(BASE_DIR, "data", "raw", "data_hero_mlbb_tier.csv")

try:
    # 1. Baca File HTML
    with open(input_file, 'r', encoding='utf-8') as file:
        html_source = file.read()
    
    # 2. Jalankan Fungsi Ekstraksi
    data = extract_hero_data_from_html(html_source)
    
    if data:
        # 3. Buat DataFrame
        df = pd.DataFrame(data)
        
        # Tampilkan preview di terminal
        print("\nPreview Data:")
        print(df.head())
        print(f"Total hero ditemukan: {len(data)}")

        # 4. Simpan ke CSV (Mode Append)
        # Cek apakah file sudah ada agar tidak menimpa header
        file_exists = os.path.isfile(output_file)
        
        df.to_csv(output_file, index=False, mode='a', header=not file_exists)
        print(f"\n[SUKSES] Data disimpan ke '{output_file}'")
        
    else:
        print("\n[INFO] Tidak ada data yang bisa disimpan.")

except FileNotFoundError:
    print(f"Error: File '{input_file}' tidak ditemukan. Silakan buat file tersebut dan paste HTML-nya.")
except Exception as e:
    print(f"Terjadi kesalahan: {e}")