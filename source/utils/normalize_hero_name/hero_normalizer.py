import re

def normalize_hero_name(hero_name: str) -> str:
   if hero_name is None:
      return None
   
   # lowercase
   name = hero_name.lower().strip()
   
   # hilangkan suffix tahun
   name = re.sub(r"\s\d{4}$", "", name)
   
   # samakan tanda hubung
   name = name.replace("-", " ")
   
   # hilangkan spasi ganda
   name = re.sub(r"\s+", " ", name)
   
   return name