import numpy as np

def calculate_avg_counter_score(hero_name, enemy_team_list, counter_dict):
   """
   Menghitung rata-rata skor ancaman.
   Logika: Seberapa terancam 'hero_name' oleh 'enemy_team_list'?
   """
   
   if not isinstance(enemy_team_list, (list, np.ndarray)) or not hero_name:
      return 0.0
   
   if len(enemy_team_list) == 0:
      return 0.0
   
   total_score = 0
   count = 0
   
   for enemy in enemy_team_list:
      # lookup: (Hero Kita, Hero Musuh)
      # jika tidak ada data counter, anggap skor 0 (netral)
      score = counter_dict.get((hero_name, enemy), 0.0)
      total_score += score
      count += 1
      
   if count == 0:
      return 0.0
   
   return round(total_score / count, 2)