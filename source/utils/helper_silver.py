import numpy as np
import re



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