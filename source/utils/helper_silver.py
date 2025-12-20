import numpy as np
import re

def normalize_name_strict(text):
   """
   Normalisasi super ketat:
   1. Lowercase
   2. Hapus Spasi, Titik, Strip, Koma
   3. Hapus angka tahun (2024)
   Contoh: "Yi Sun-shin" -> "yisunshin", "Yi Sun Shin" -> "yisunshin"
   """
   if not isinstance(text, str): return ""
   clean = text.lower()
   # Hapus semua yg bukan huruf/angka
   clean = re.sub(r'[^a-z0-9]', '', clean)

   return clean

def calculate_avg_counter_score(hero_name, enemy_team_list, counter_dict):
   """
   Logika: Seberapa kuat 'hero_name' meng-counter 'enemy_team_list'?
   Lookup Key: (Musuh, Saya) -> Artinya Musuh dicounter oleh Saya.
   """
   if not isinstance(enemy_team_list, (list, np.ndarray)) or not hero_name:
      return 0.0
   
   if len(enemy_team_list) == 0:
      return 0.0
   
   total_score = 0
   count = 0
   
   for enemy in enemy_team_list:
      # --- PERUBAHAN UTAMA DISINI ---
      # Kita cek: Apakah Enemy (Target) di-counter oleh Hero_Name (Counter)?
      # Key Dictionary: (Target, Counter)
      
      # Opsi A: Offensive Score (Keuntungan Saya) -> REKOMENDASI SAYA
      score = counter_dict.get((enemy, hero_name), 0.0)
      
      # Opsi B: Net Score (Keuntungan - Kerugian) -> Lebih Komplex
      # score_off = counter_dict.get((enemy, hero_name), 0.0)
      # score_def = counter_dict.get((hero_name, enemy), 0.0)
      # score = score_off - score_def 

      total_score += score
      count += 1
      
   if count == 0:
      return 0.0
   
   return round(total_score / count, 2)