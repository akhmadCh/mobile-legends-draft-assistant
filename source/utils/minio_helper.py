from minio import Minio
import io
import pandas as pd
import os 

MINIO_CONFIG = {
   'endpoint' : 'localhost:9000',
   'access_key' : 'minioadmin',
   'secret_key' : 'minioadmin',
   'secure' : False
}

def get_minio_client():
   return Minio(**MINIO_CONFIG)

def upload_df_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str, file_format='csv'):
   """
   upload dataframe pandas langsung ke MinIO sebagai file CSV.
   """
   client = get_minio_client()
   
   # pastikan bucket ada
   if not client.bucket_exists(bucket_name):
      client.make_bucket(bucket_name)
   
   # convert df sebagai csv ke buffer memory
   data_stream = io.BytesIO()
   if file_format == 'csv':
      df.to_csv(data_stream, index=False)
      content_type = 'text/csv'
   elif file_format == 'parquet':
      df.to_parquet(data_stream, index=False)
      content_type = 'application/octet-stream'
   
   # Reset pointer stream ke awal
   data_stream.seek(0)
   
   try:
      client.put_object(
         bucket_name,
         object_name,
         data_stream,
         length=data_stream.getbuffer().nbytes,
         content_type=content_type
      )
      print(f"[MINIO] Berhasil Upload: {bucket_name}/{object_name}")
   except Exception as e:
      print(f"[MINIO] Error Upload: {object_name}: {e}")