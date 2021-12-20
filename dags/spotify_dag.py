import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.append("/home/georgy/pythonProjects/MySpotiyfTracking/tasks")
from spotify_etl import run_spotify_etl

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(0, 0, 0, 0),
        'email': ['thegeorgy@icloud.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=1)
        }

dag = DAG(
        'spotify_dag',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
        )

run_task = PythonOperator(
        task_id='spotify_etl',
        python_callable=run_spotify_etl,
        dag=dag
        )

run_task
