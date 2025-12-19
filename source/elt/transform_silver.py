import pandas as pd
import os
import re

BASE_DIR = os.getcwd()
BRONZE_MATCH_PATH = os.path.join(BASE_DIR, "data", "bronze", "mpl_matches.parquet")
SILVER_DATA_PATH = os.path.join(BASE_DIR, "data", "silver", "training_data.parquet")

def clean_hero_name(name):
    """Membersihkan nama hero dari tahun/versi."""
    if not isinstance(name, str): return "Unknown"
    # Hapus angka tahun (4 digit) dan karakter setelahnya
    name = re.sub(r'\s*\d{4}.*', '', name)
    return name.strip()

def main():
    print("🔄 TRANSFORM: Kembali ke One-Hot Encoding (Nama Hero)...")
    
    if not os.path.exists(BRONZE_MATCH_PATH):
        print("❌ File Bronze tidak ditemukan.")
        return

    df_match = pd.read_parquet(BRONZE_MATCH_PATH)
    print(f"   Total Match: {len(df_match)}")

    # 1. Cleaning & Splitting
    # Split kolom 'Left_Picks' dan 'Right_Picks'
    def split_heroes(row, col_name):
        picks = str(row[col_name]).split(',')
        heroes = [clean_hero_name(h) for h in picks[:5]]
        # Padding jika kurang dari 5
        while len(heroes) < 5: heroes.append("Unknown")
        return heroes

    # Buat list list hero
    t1_lists = df_match.apply(lambda r: split_heroes(r, 'Left_Picks'), axis=1)
    t2_lists = df_match.apply(lambda r: split_heroes(r, 'Right_Picks'), axis=1)

    # 2. One-Hot Encoding Manual
    # Kita kumpulkan semua nama hero unik dulu
    all_heroes = set()
    for heroes in t1_lists: all_heroes.update(heroes)
    for heroes in t2_lists: all_heroes.update(heroes)
    
    if "Unknown" in all_heroes: all_heroes.remove("Unknown")
    sorted_heroes = sorted(list(all_heroes))
    print(f"   Total Hero Unik: {len(sorted_heroes)}")

    # Buat list dictionary untuk DataFrame
    data_rows = []
    
    for idx, row in df_match.iterrows():
        # Label Winner (1 jika Kiri Menang, 0 jika Kanan Menang)
        label = 1 if row['Winner_Match'] == row['Team_Left'] else 0
        
        # Fitur: T1_HeroName dan T2_HeroName
        row_feat = {'Label_Winner': label}
        
        # Isi 0 dulu untuk semua hero
        for h in sorted_heroes:
            row_feat[f"T1_{h}"] = 0
            row_feat[f"T2_{h}"] = 0
            
        # Isi 1 jika hero dipick
        for h in t1_lists[idx]:
            if h in sorted_heroes: row_feat[f"T1_{h}"] = 1
            
        for h in t2_lists[idx]:
            if h in sorted_heroes: row_feat[f"T2_{h}"] = 1
            
        data_rows.append(row_feat)

    # 3. Simpan
    final_df = pd.DataFrame(data_rows)
    os.makedirs(os.path.dirname(SILVER_DATA_PATH), exist_ok=True)
    final_df.to_parquet(SILVER_DATA_PATH)
    print(f"✅ Selesai! Data Training tersimpan di: {SILVER_DATA_PATH}")
    print(f"   Dimensi Data: {final_df.shape}")

if __name__ == "__main__":
    main()