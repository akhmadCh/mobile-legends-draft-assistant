from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'tim_data_mlbb',
   'depends_on_past': False,
   'email_on_failure': False,
   'retries': 0,
}

PROJECT_ROOT = '/opt/airflow'

with DAG(
   '02_user_learning_pipeline', 
   default_args=default_args,
   description='Pipeline cepat untuk memproses history user (Raw -> Gold)',
   # Jalankan setiap jam (atau sesuaikan kebutuhan, misal '*/30 * * * *' per 30 menit)
   schedule_interval='0 * * * *', 
   start_date=datetime(2024, 1, 1),
   catchup=False,
   tags=['mlbb', 'user-data', 'frequent']
) as dag:

   # Satu task menjalankan script python yang berisi flow Bronze->Silver->Gold sekaligus
   # agar lebih cepat dan tidak boros task overhead.
   task_process_user_data = BashOperator(
      task_id='process_user_etl',
      bash_command=f'cd {PROJECT_ROOT} && python -m source.transform.process_user_data'
   )

   task_process_user_data