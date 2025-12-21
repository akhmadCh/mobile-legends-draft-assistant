import numpy as np
import re

def normalize_name_strict(text):
   if not isinstance(text, str): return ""
   clean = text.lower()
   clean = re.sub(r'[^a-z0-9]', '', clean) 
   return clean

def calculate_avg_counter_score(hero_name, enemy_team_list, counter_dict):
   hero_norm = normalize_name_strict(hero_name)
   musuh_norm = [normalize_name_strict(m) for m in enemy_team_list]
   
   total = 0
   count = 0
   
   for musuh in musuh_norm:
      # score = counter_dict.get((hero_norm, musuh), 0.0)
      
      # Opsi B: Net Score (Keuntungan - Kerugian) -> Lebih Komplex
      # apakah saya counter musuh +
      score_advantage = counter_dict.get((hero_norm, musuh), 0.0)
      # apakah musuh counter saya -
      score_disadvantage = counter_dict.get((musuh, hero_norm), 0.0)
      
      score = score_advantage - score_disadvantage
      
      total += score
      count += 1
      
   avg = round(total / count, 2) if count > 0 else 0
   
   return avg

# def calculate_avg_counter_score(hero_name, enemy_team_list, counter_dict):
#    """
#    Logika: Seberapa kuat 'hero_name' meng-counter 'enemy_team_list'?
#    Lookup Key: (Musuh, Saya) -> Artinya Musuh dicounter oleh Saya.
#    """
#    if not isinstance(enemy_team_list, (list, np.ndarray)) or not hero_name:
#       return 0.0
   
#    if len(enemy_team_list) == 0:
#       return 0.0
   
#    total_score = 0
#    count = 0
   
#    hero_clean = str(hero_name).strip().lower()
   
#    for enemy in enemy_team_list:
#       enemy_clean = str(enemy).strip().lower()
      
#       # cek 1 arah
#       # score = counter_dict.get((enemy, hero_name), 0.0)
      
#       # Opsi B: Net Score (Keuntungan - Kerugian) -> Lebih Komplex
#       # apakah saya counter musuh +
#       score_advantage = counter_dict.get((hero_clean, enemy_clean), 0.0)
#       # apakah musuh counter saya -
#       score_disadvantage = counter_dict.get((enemy_clean, hero_clean), 0.0)
      
#       score = score_advantage - score_disadvantage

#       total_score += score
#       count += 1
      
#    if len(enemy_team_list) == 0: return 0.0
   
#    return round(total_score / len(enemy_team_list))