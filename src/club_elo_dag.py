"""
DAG for club_elo update
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from workloads import club_elo_api, club_elo

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'club_elo_dag',
    default_args=default_args,
    description='A simple DAG to run club_elo_api and club_elo consecutively',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


# Define the first task
run_club_elo_api_task = PythonOperator(
    task_id='run_club_elo_api',
    python_callable=club_elo_api.main,
    dag=dag,
)

# Define the second task
run_club_elo_task = PythonOperator(
    task_id='run_club_elo',
    python_callable=club_elo.main,
    dag=dag,
)

# Set the task dependencies
run_club_elo_api_task >> run_club_elo_task
