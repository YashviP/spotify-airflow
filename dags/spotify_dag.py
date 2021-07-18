from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from spotify_etl import spotify_etl_run

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,7,17,5,46,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="My first DAG with ETL process!",
    schedule_interval=timedelta(days=1),
)


run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable=spotify_etl_run,
    dag=dag,
)

run_etl
