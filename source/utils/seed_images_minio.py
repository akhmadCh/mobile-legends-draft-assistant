from minio import Minio
import pandas as pd
import requests
import os
import io

# ================= KONEKSI MINIO =================
# Perhatikan: Saat script ini jalan DI DALAM Docker, hostnya 'minio'.
# Tapi kalau dijalankan DI TERMINAL LAPTOP, hostnya 'localhost'.
# Kita buat dinamis biar aman.

MINIO_CONF = {
   "endpoint": "localhost:9000", # Ganti ke 'minio:9000' jika run via Airflow/Docker
   "access_key": "minioadmin",
   "secret_key": "minioadmin",
   "secure": False
}

BUCKET_NAME = "hero-icons"

# ================= PATH DATA =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

def get_minio_client():
   return Minio(
      MINIO_CONF["endpoint"],
      access_key=MINIO_CONF["access_key"],
      secret_key=MINIO_CONF["secret_key"],
      secure=MINIO_CONF["secure"]
   )

def seed_hero_images():
   print("=== MULAI UPLOAD GAMBAR KE MINIO ===")
   
   # 1. Load Data Hero Bersih
   parquet_path = os.path.join(SILVER_DIR, "hero_master_clean.parquet")
   if not os.path.exists(parquet_path):
      print("ERROR: Data Silver tidak ditemukan. Transform dulu!")
      return
      
   df = pd.read_parquet(parquet_path)
   hero_list = df['Nama Hero'].unique()
   print(f"-> Ditemukan {len(hero_list)} hero untuk dicari gambarnya.")

   # 2. Setup MinIO Bucket
   client = get_minio_client()
   if not client.bucket_exists(BUCKET_NAME):
      client.make_bucket(BUCKET_NAME)
      print(f"-> Bucket '{BUCKET_NAME}' berhasil dibuat.")
   else:
      print(f"-> Bucket '{BUCKET_NAME}' sudah ada.")

   # 3. Loop Download & Upload
   # Sumber gambar: Kita gunakan repo publik yang biasanya lengkap (contoh: dxta/mlbb-api atau sejenisnya)
   # URL Pattern contoh: https://raw.githubusercontent.com/dxta/mlbb-api/main/images/heroes/Lancelot.png
   # Kita harus pintar-pintar memformat nama hero agar sesuai URL (biasanya lowercase/tanpa spasi aneh)
   
   BASE_URL_IMG = "https://raw.githubusercontent.com/dxta/mlbb-api/main/images/heroes/"
   
   success_count = 0
   
   for hero_name in hero_list:
      # Format nama untuk URL (Contoh: "Yi Sun-shin" -> "Yi-Sun-shin" atau sesuai sumber)
      # Mari coba format standar: Ganti spasi dengan strip, atau langsung nama asli
      # Sumber dxta biasanya sensitif. Mari coba download dengan nama asli dulu.
      
      # Opsi: Gunakan placeholder jika gagal
      try:
         # Bersihkan nama untuk URL (Misal: Chou -> Chou)
         formatted_name = hero_name.replace(" ", "-").replace("'", "")
         img_url = f"{BASE_URL_IMG}{formatted_name}.png"
         
         # Download Image ke Memory (RAM)
         response = requests.get(img_url)
         
         if response.status_code == 200:
            # Convert ke Bytes
            img_data = io.BytesIO(response.content)
            img_size = len(response.content)
            
            # Upload ke MinIO
            # Nama file di MinIO: "Lancelot.png"
            object_name = f"{hero_name}.png"
            
            client.put_object(
               BUCKET_NAME,
               object_name,
               img_data,
               img_size,
               content_type="image/png"
            )
            print(f"   [OK] {hero_name} terupload.")
            success_count += 1
         else:
            print(f"   [404] Gambar tidak ditemukan untuk: {hero_name} (URL: {img_url})")
            # Disini Anda bisa upload gambar "dummy/tanda tanya" sebagai default
            
      except Exception as e:
            print(f"   [ERR] Gagal memproses {hero_name}: {e}")

   print(f"=== SELESAI. Berhasil upload {success_count}/{len(hero_list)} gambar. ===")

if __name__ == "__main__":
   seed_hero_images()