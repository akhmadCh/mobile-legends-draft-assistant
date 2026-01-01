from minio import Minio
import io
import pandas as pd
import os 

MINIO_CONFIG = {
   # 'endpoint': os.getenv("MINIO_ENDPOINT", "localhost:9000"),
   'endpoint': os.getenv("MINIO_ENDPOINT", "minio:9000"),
   'access_key': os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
   'secret_key': os.getenv("MINIO_SECRET_KEY", "minioadmin"),
   'secure': False
}

# MINIO_CONFIG = {
#    'endpoint' : 'localhost:9000',
#    'access_key' : 'minioadmin',
#    'secret_key' : 'minioadmin',
#    'secure' : False
# }

def get_minio_client():
   # Debugging (Opsional): Print endpoint yang sedang dipakai
   return Minio(**MINIO_CONFIG)

def upload_df_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str, file_format='csv'):
   """
   upload dataframe pandas langsung ke MinIO sebagai file CSV.
   """
   client = get_minio_client()
   
   # pastikan bucket ada
   if not client.bucket_exists(bucket_name):
      client.make_bucket(bucket_name)
   
   if file_format == 'csv':
      # convert df sebagai csv ke buffer memory
      csv_bytes = df.to_csv(index=False).encode('utf-8')
      data_stream = io.BytesIO(csv_bytes)
      content_type = 'application/csv'
      length = len(csv_bytes)
   elif file_format == 'sql':
      # convert df sebagai csv ke buffer memory
      data_stream = io.BytesIO()
      
      df.to_sql(object_name, data_stream, index=False)
      content_type = 'application/octet-stream'
   elif file_format == 'parquet':
      # convert df sebagai csv ke buffer memory
      data_stream = io.BytesIO()
      
      # parquet butuh engine pyarrow atau fastparquet
      parquet_bytes = df.to_parquet(index=False) 
      data_stream = io.BytesIO(parquet_bytes)
      length = data_stream.getbuffer().nbytes
      content_type = 'application/octet-stream'
   
   # Reset pointer stream ke awal
   data_stream.seek(0)
   
   try:
      client.put_object(
         bucket_name,
         object_name,
         data_stream,
         length,
         content_type=content_type
      )
      print(f"[MINIO] Berhasil Upload: {bucket_name}/{object_name}")
   except Exception as e:
      print(f"[MINIO] Error Upload: {object_name}: {e}")

def read_df_from_minio(bucket_name: str, object_name: str, file_format='csv'):
   client = get_minio_client()
   
   try:
      response = client.get_object(bucket_name, object_name)
      
      if file_format == 'csv':
         df = pd.read_csv(response)
      elif file_format == 'parquet':
         # agar bisa di-seek oleh engine parquet
         data_buffer = io.BytesIO(response.read())
         df = pd.read_parquet(data_buffer)
      
      return df
   except Exception as e:
      print(f"[MINIO] Error Read: {object_name}: {e}")
      return None