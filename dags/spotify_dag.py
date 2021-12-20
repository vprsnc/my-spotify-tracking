from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from myspotifytracking import spotify_etl

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
        descripiton="daily tracking of tracks form yesterday",
        schedule_interval=timedelta(days=1)
        )



