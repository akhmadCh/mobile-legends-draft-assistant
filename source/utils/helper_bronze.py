import re
from datetime import datetime

HERO_ALIAS = {
   "wu ze tian": "zetian",
   "wuzetian": "zetian",
   "wu zhe tian": "zetian",
}

def get_timestamp():
   """return waktu saat ini untuk metadata"""
   return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def normalize_hero_name(name: str) -> str:
   if not isinstance(name, str):
      return None

   name = name.lower().strip()
   name = name.replace("-", " ")

   # hapus tahun dan versi
   name = re.sub(r'(?:\d{4}v?\d*|v\d+)$', '', name)

   # bersihkan karakter tapi tetap simpan spasi
   name = re.sub(r'[^a-z ]', '', name)
   name = re.sub(r'\s+', ' ', name).strip()

   # alias HARUS di sini
   name = HERO_ALIAS.get(name, name)

   # canonical key tanpa spasi
   return name.replace(" ", "")


def clean_percentage(value):
   """ubah '50%' (str) jadi 50.0 (float)"""
   if isinstance(value, str):
      return float(value.replace('%', '').strip())
   return value

def parse_hero_list(hero_str: str):
   if not isinstance(hero_str, str):
      return []

   heroes = hero_str.split(",")
   return [normalize_hero_name(h.strip()) for h in heroes if h.strip()]

def get_tier_score(tier):
   """konversi tier ke skor angka"""
   scores = {
      'SS': 5,
      'S': 4,
      'A': 3,
      'B': 2,
      'C': 1,
   }
   # return 0 jika tidak sesuai
   return scores.get(tier, 0)