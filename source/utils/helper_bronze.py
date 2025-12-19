import re

def normalize_hero_name(name: str) -> str:
   if not isinstance(name, str):
      return None

   name = name.lower().strip()
   name = re.sub(r"\s\d{4}$", "", name)
   name = name.replace("-", " ")
   name = re.sub(r"\s+", " ", name)

   return name

def parse_hero_list(hero_str: str):
   if not isinstance(hero_str, str):
      return []

   heroes = hero_str.split(",")
   return [normalize_hero_name(h.strip()) for h in heroes if h.strip()]
