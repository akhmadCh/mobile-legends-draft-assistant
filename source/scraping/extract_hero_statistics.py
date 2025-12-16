import pandas as pd
from bs4 import BeautifulSoup
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def extract_hero_statistics(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    data_list = []
    
    # 1. AMBIL HEADER (Nama & Rank)
    headers = soup.find_all('div', class_='p-1.5 flex items-center space-x-2 border-b border-[#393E46]')
    
    # 2. AMBIL STATS GRIDS
    stats_grids = soup.find_all('div', class_='grid grid-cols-2 gap-1')
    
    # Validasi jumlah
    if len(headers) != len(stats_grids):
        print(f"[WARNING] Jumlah Hero ({len(headers)}) != Stats ({len(stats_grids)}).")
    
    # 3. LOOPING ZIP
    for header, stats in zip(headers, stats_grids):
        
        # --- Bagian Nama & Rank (Sudah Oke) ---
        rank_tag = header.find('span', class_='text-silver')
        rank = rank_tag.get_text(strip=True) if rank_tag else 'N/A'
        
        name_tag = header.find('h3') # Selector lebih umum agar aman
        name = name_tag.get_text(strip=True) if name_tag else 'N/A'
        
        hero_stats = {
            'Rank': rank,
            'Nama Hero': name,
            'Win Rate': 'N/A',
            'Pick Rate': 'N/A',
            'Ban Rate': 'N/A',
            'Role': 'N/A',
            'Lane': 'N/A',
            'Speciality': 'N/A'
        }
        
        # --- Bagian Statistik (PERBAIKAN DISINI) ---
        # Kita loop setiap "kotak kecil" di dalam grid
        # Class wrapper kotak kecil:
        stat_items = stats.find_all('div', class_='flex flex-col bg-[#2A2F37] p-1 rounded')
        
        for item in stat_items:
            # A. Cari Label Judul (Win Rate, Role, dll)
            # Class: text-[10px] font-medium text-silver opacity-80
            label_tag = item.find('span', class_=lambda x: x and 'text-silver' in x and 'opacity-80' in x)
            
            if not label_tag:
                continue
                
            label_text = label_tag.get_text(strip=True)
            
            # B. Ambil Container Isi (div class="mt-0.5")
            value_container = item.find('div', class_='mt-0.5')
            if not value_container:
                continue

            # C. Ekstraksi berdasarkan tipe Label
            if label_text in ['Win Rate', 'Pick Rate', 'Ban Rate']:
                # Cari span yang punya class 'font-medium text-sm' (tanpa peduli warna emerald/blue/purple)
                val_span = value_container.find('span', class_=lambda x: x and 'font-medium text-sm' in x)
                if val_span:
                    hero_stats[label_text] = val_span.get_text(strip=True)
            
            elif label_text in ['Role', 'Lane', 'Speciality']:
                # Cari semua pill (span dengan background gelap rounded)
                # Biasanya class mengandung 'rounded-full' dan 'bg-[#393E46]'
                pills = value_container.find_all('span', class_=lambda x: x and 'rounded-full' in x)
                if pills:
                    # Gabungkan teks dengan koma (misal: "Exp Lane, Roam")
                    hero_stats[label_text] = ", ".join([p.get_text(strip=True) for p in pills])

        data_list.append(hero_stats)
        
    return data_list

# --- EKSEKUSI ---
input_file = 'input_statistik_hero.html'
# output_file = 'statistik_hero.csv'
output_file = os.path.join(BASE_DIR, "data", "raw", "data_statistik_hero.csv")

try:
    with open(input_file, 'r', encoding='utf-8') as file:
        html_source = file.read()
    
    print("Mengekstrak data...")
    data = extract_hero_statistics(html_source)
    
    if data:
        df = pd.DataFrame(data)
        print("\n--- Preview Hasil ---")
        print(df.to_string(index=False, max_rows=5)) # Tampilkan tabel lebih rapi di terminal
        
        file_exists = os.path.isfile(output_file)
        df.to_csv(output_file, index=False, mode='a', header=not file_exists)
        print(f"\n[SUKSES] {len(data)} data disimpan ke '{output_file}'")
    else:
        print("[GAGAL] Tidak ada data ditemukan.")

except FileNotFoundError:
    print("File input.html tidak ada.")