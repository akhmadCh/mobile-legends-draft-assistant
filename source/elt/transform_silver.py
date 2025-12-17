import pandas as pd
import os
import re
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

# ==========================================
# HELPERS FUNCTION FOR CLEANING AND TRANSFORMING
# ==========================================

def clean_hero_name(name):
   """
   Membersihkan nama hero yang 'kotor' dari hasil scraping.
   Contoh: 
   - "Lancelot 2021" -> "Lancelot"
   - "Yi Sun-Shin" -> "Yi Sun-shin" (Standarisasi Title Case)
   - " Chang'e " -> "Chang'e" (Hapus spasi)
   """
   if not isinstance(name, str):
      return "Unknown"
   
   # hapus spasi di awal/akhir
   name = name.strip()
   
   # hapus tahun atau angka di belakang " 2021" " 2024"
   # regex: \s (spasi) + \d+ (angka digit) + $ (akhir string)
   # akan menghapus " 2020 V2", " 2024", " V2"
   name = re.sub(r'\s+(?:20\d{2}|V\d+).*$', '', name)
   
   # hapus angka sisa akhir
   name = re.sub(r'\s+\d+$', '', name)
   
   name = name.title()
   
   # dictionary manual
   corrections = {
      "Yi Sun-Shin": "Yi Sun-shin",  # Memperbaiki kapitalisasi S
      "Wu Zetian": "Zetian",         # Mencoba mapping ke nama yang mungkin ada di Counter
      "Popol And Kupa": "Popol and Kupa",
      "Change": "Chang'e",           # Jaga-jaga jika ada typo
      "Chang'E": "Chang'e"
   }
   
   return corrections.get(name, name)

def parse_percentage(val):
   """
   Mengubah string "55.5%" menjadi float 0.555
   """
   if isinstance(val, str):
      val = val.replace('%', '').strip()
      try:
         return float(val) / 100.0
      except ValueError:
         return 0.0
   return val

# ==========================================
# TRANSFORMATION
# ==========================================

def process_hero_stats():
   """
   Transformasi 1: Membersihkan Data Master Hero (SQL)
   """
   print("[SILVER] Memproses Hero Stats...")
   input_path = os.path.join(BRONZE_DIR, "hero_stats.parquet")
   
   if not os.path.exists(input_path):
      print(f"Skipping: {input_path} tidak ditemukan.")
      return None

   df = pd.read_parquet(input_path)
   
   # bersihkan nama hero (kunci untuk JOIN nanti)
   df['Nama Hero'] = df['Nama Hero'].apply(clean_hero_name)
   
   # Type Casting: persen % ke Float
   cols_to_fix = ['Win Rate', 'Pick Rate', 'Ban Rate']
   for col in cols_to_fix:
      if col in df.columns:
         df[col] = df[col].apply(parse_percentage)
      
   # simpan datanya
   output_path = os.path.join(SILVER_DIR, "hero_master_clean.parquet")
   df.to_parquet(output_path, index=False)
   print(f"   -> Selesai. Disimpan ke {output_path}")
   return df

def process_matches():
   """
   Transformasi 2: Membersihkan & Feature Engineering Data Match (MPL)
   Ini adalah bagian terberat: String Split -> Clean Name -> One Hot Encoding
   """
   print("[SILVER] Memproses Match History (MPL ID & PH)...")
   input_path = os.path.join(BRONZE_DIR, "mpl_matches.parquet")
   
   if not os.path.exists(input_path):
      print(f"Skipping: {input_path} tidak ditemukan.")
      return
   
   df = pd.read_parquet(input_path)
   
   # --- PARSING STRING KE LIST ---
   # data di CSV bentuknya string "HeroA, HeroB, HeroC", pecah jadi list python.
   # sekaligus menerapkan fungsi 'clean_hero_name'
   
   def parse_and_clean_picks(pick_string):
      if not isinstance(pick_string, str): return []

      raw_list = pick_string.split(',') 

      clean_list = [clean_hero_name(h) for h in raw_list if h.strip() != ""]
      return clean_list

   # Terapkan ke kolom Picks dan Bans
   target_cols = ['Left_Picks', 'Right_Picks', 'Left_Bans', 'Right_Bans']
   for col in target_cols:
      df[col + '_List'] = df[col].apply(parse_and_clean_picks)

   # --- ONE-HOT ENCODING (WIDE FORMAT) ---
   # membuat kolom baru T1_HeroName (1/0) dan T2_HeroName (1/0)
   # T1 = Team Left, T2 = Team Right
   
   print("   -> Melakukan One-Hot Encoding (Sabar, ini agak rumit)...")
   
   # ambil semua hero unik dari dataset untuk dijadikan nama kolom
   all_heroes = set()
   for picks in df['Left_Picks_List']: all_heroes.update(picks)
   for picks in df['Right_Picks_List']: all_heroes.update(picks)
   
   # sorting sesuai abjad
   sorted_heroes = sorted(list(all_heroes))
   
   # dictionary kosong untuk menampung data one-hot
   # struktur {'T1_Ling': [0, 1, 0...], 'T2_Fanny': [1, 0, 0...]}
   one_hot_data = {}
   
   # inisialisais kolom
   for hero in sorted_heroes:
      one_hot_data[f"T1_{hero}"] = []
      one_hot_data[f"T2_{hero}"] = []
      
   # isi datanya dengan iterasi per baris match
   for _, row in df.iterrows():
      # ambil list pick tim kiri dan kanan
      picks_left = set(row['Left_Picks_List'])
      picks_right = set(row['Right_Picks_List'])
      
      for hero in sorted_heroes:
         # cek apakah hero ini dipick Tim Kiri?
         one_hot_data[f"T1_{hero}"].append(1 if hero in picks_left else 0)
         # cek apakah hero ini dipick Tim Kanan?
         one_hot_data[f"T2_{hero}"].append(1 if hero in picks_right else 0)
      
   # gabungkan data one-hot ke DataFrame utama
   df_one_hot = pd.DataFrame(one_hot_data)
   df_final = pd.concat([df, df_one_hot], axis=1)
   
   # --- LABEL ENCODING (TARGET VARIABLE) ---
   # Target prediksi: Apakah Team Left Menang? (1 = Ya, 0 = Tidak)
   # Logika: Jika Winner_Match == Team_Left, maka 1.
   
   # bersihkan nama tim (trim spasi)
   df_final['Team_Left'] = df_final['Team_Left'].str.strip()
   df_final['Winner_Match'] = df_final['Winner_Match'].str.strip()
   
   df_final['Label_Winner'] = np.where(df_final['Winner_Match'] == df_final['Team_Left'], 1, 0)
   
   # --- SIMPAN HASIL ---
   # simpan kolom penting saja untuk ML
   # buang kolom 'Left_Picks' string asli biar file tidak berat, karena sudah ada one-hot
   
   output_path = os.path.join(SILVER_DIR, "training_data.parquet")
   df_final.to_parquet(output_path, index=False)
   
   print(f"   -> Selesai. Dataset ML tersimpan di {output_path}")
   print(f"   -> Jumlah Kolom Fitur: {len(sorted_heroes) * 2} (T1 & T2)")
   print(f"   -> Total Baris Data: {len(df_final)}")

if __name__ == "__main__":
   print("=== MULAI PROSES TRANSFORMASI (BRONZE -> SILVER) ===")
   process_hero_stats()
   process_matches()
   print("=== TRANSFORMASI SELESAI ===")