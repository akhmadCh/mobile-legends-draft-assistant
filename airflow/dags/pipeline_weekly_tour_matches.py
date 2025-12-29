from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'tim_data_mlbb',
   'depends_on_past': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

PROJECT_ROOT = '/opt/airflow'

with DAG(
   '02_weekly_match_pipeline',
   default_args=default_args,
   description='Pipeline Mingguan untuk update Match MPL & Retrain Model',
   # schedule_interval='0 6 * * 1', # Cron: Setiap Senin jam 06:00 Pagi
   schedule_interval=None, # None: karena MPL S16 sudah selesai, dan menunggu S17
   start_date=datetime(2024, 1, 1),
   catchup=False,
   tags=['mlbb', 'weekly', 'training']
) as dag:

   # --- STEP 1: SCRAPE MATCH BARU ---
   task_scrape_matches = BashOperator(
      task_id='scrape_matches_mpl',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.scraping.scrape_matches'
   )

   # --- STEP 2: RE-PROCESS DATA ---
   # Kita perlu jalankan ulang flow transformasi agar match baru masuk ke sistem
   
   task_process_bronze = BashOperator(
      task_id='update_bronze',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_bronze'
   )

   task_process_silver = BashOperator(
      task_id='update_silver',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_silver'
   )

   task_process_gold = BashOperator(
      task_id='update_gold',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_gold3'
   )

   # --- STEP 3: MACHINE LEARNING ---
   # Latih ulang model karena ada data match baru
   
   task_train_model = BashOperator(
      task_id='retrain_xgboost_model',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.ml.train_model3'
   )

   # --- ALUR FLOW ---
   # Scrape -> Bronze -> Silver -> Gold -> Training
   task_scrape_matches >> task_process_bronze >> task_process_silver >> task_process_gold >> task_train_model