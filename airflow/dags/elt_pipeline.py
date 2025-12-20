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

   # Task 1: Ingest (Raw -> Bronze)
   task_ingest = BashOperator(
      task_id='ingest_to_bronze',
      bash_command='python /opt/airflow/source/elt/ingest_bronze.py', 
   )

   # Task 2: Transform (Bronze -> Silver)
   task_transform_silver = BashOperator(
      task_id='transform_to_silver',
      bash_command='python /opt/airflow/source/transform/process_silver.py',
   )
   
   # Task 3: Transform (Silver -> Gold)
   task_transform_gold = BashOperator(
      task_id='transform_to_gold',
      bash_command='python /opt/airflow/source/transform/process_gold.py',
   )

   # Urutan Eksekusi
   task_ingest >> task_transform_silver >> task_transform_gold