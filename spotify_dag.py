from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(0, 0, 0, 0, 0),
        'email': ['test_email@airflow.com'],
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
        }

dag = DAG(
        'spotify_dag',
        default_args=default_args,
        description='Expample dag for spotify',
        schedule_interval=timedelta(days=1)
        )


def just_a_func():
    print("I'm going to schedule smth")


run_etl = PythonOperator(
        task_id='spotify_etl',
        python_callable=run_spotify_etl,
        dag=dag
        )

run_etl
