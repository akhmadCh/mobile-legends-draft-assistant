from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- KONFIGURASI UMUM ---
# Pengaturan dasar agar kalau gagal, dia coba lagi 1x setelah 5 menit
default_args = {
   'owner': 'tim_data_mlbb',
   'depends_on_past': False,
   'email_on_failure': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

PROJECT_ROOT = '/opt/airflow'

# Definisi DAG
with DAG(
   '01_hero_intelligence_pipeline', # Nama unik DAG
   default_args=default_args,
   description='Pipeline rutin 2 hari sekali untuk update data Hero & Meta',
   schedule_interval='0 0 */2 * *', # Cron: Setiap 2 hari jam 00:00
   start_date=datetime(2024, 1, 1),
   catchup=False, # Jangan jalankan jadwal masa lalu yang terlewat
   tags=['mlbb', 'daily', 'bronze-silver-gold']
) as dag:

   # --- STEP 1: SCRAPING PARALEL ---
   # Kita scrape 3 sumber data sekaligus biar cepat
   
   task_scrape_stats = BashOperator(
      task_id='scrape_hero_stats',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.scraping.scrape_hero_stats'
   )

   task_scrape_meta = BashOperator(
      task_id='scrape_meta_tier',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.scraping.scrape_meta_tier'
   )

   task_scrape_counter = BashOperator(
      task_id='scrape_counter',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.scraping.scrape_counter_hero'
   )

   # --- STEP 2: MOCKING DB & INGESTION (Simulasi Internal Data) ---
   # Setelah stats didapat, kita pura-pura masukkan ke SQL DB dulu
   
   task_mock_db = BashOperator(
      task_id='generate_mock_sql_db',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.utils.init_db'
   )

   task_ingest_sql = BashOperator(
      task_id='ingest_sql_to_bronze',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.ingestion.ingest_internal_sql'
   )

   # --- STEP 3: PROCESS LAYERS (Bronze -> Silver -> Gold) ---
   
   task_process_bronze = BashOperator(
      task_id='process_bronze_layer',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_bronze'
   )

   task_process_silver = BashOperator(
      task_id='process_silver_layer',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_silver'
   )

   task_process_gold = BashOperator(
      task_id='process_gold_layer',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_gold'
   )

   # --- ALUR DEPENDENCY (FLOW) ---
   
   # 1. Scrape Stats selesai -> baru bisa bikin Mock DB
   task_scrape_stats >> task_mock_db >> task_ingest_sql
   
   # 2. Ingest SQL & Scrape lainnya selesai -> baru masuk Bronze
   [task_ingest_sql, task_scrape_meta, task_scrape_counter] >> task_process_bronze
   
   # 3. Bronze -> Silver -> Gold (Berurutan)
   task_process_bronze >> task_process_silver >> task_process_gold