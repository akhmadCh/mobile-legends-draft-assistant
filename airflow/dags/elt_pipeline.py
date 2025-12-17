from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definisi Default Arguments
default_args = {
   'owner': 'data_team',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

# Definisi DAG
with DAG(
   'mobile_legends_elt_pipeline',
   default_args=default_args,
   description='Pipeline ELT Data Lakehouse Mobile Legends',
   schedule_interval='@daily', # Jalankan setiap hari
   start_date=datetime(2023, 12, 1),
   catchup=False,
   tags=['uas', 'mobile_legends'],
) as dag:

   # Task 1: Ingest Data (Raw -> Bronze)
   # Kita menggunakan BashOperator untuk memanggil script python yang sudah kita buat
   task_ingest = BashOperator(
      task_id='ingest_to_bronze',
      bash_command='python /opt/airflow/src/etl/ingest.py', 
      # Path /opt/airflow adalah path di dalam Docker Container
   )

   # Task 2: Transform Data (Bronze -> Silver)
   # Nanti kita buat script transform.py, sekarang dummy dulu
   task_transform = BashOperator(
      task_id='transform_to_silver',
      bash_command='echo "Transform script will run here later"',
   )

   # Menentukan Urutan Task (Ingest DULUAN, baru Transform)
   task_ingest >> task_transform