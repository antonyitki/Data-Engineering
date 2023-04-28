from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

from spotify_etl import run_spotify_etl

now = timezone.utcnow()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': timezone.datetime(2022, 11, 23),
    'email': ['dorian.gloinec@protonmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'personnal_data_spotify_dag',
    default_args = default_args,
    description = 'DAG for SpotifyDailyDataDathering ETL',
    schedule_interval = timedelta(hours=3)
)

spotify_ETL_job = PythonOperator(
    task_id = "spotify_ETL",
    python_callable = run_spotify_etl,
    dag = dag
)

spotify_ETL_job