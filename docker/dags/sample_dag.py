from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

def hello_world():
    print("Hello World")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sample_airflow_dag',
    default_args=default_args,
    description='A simple sample DAG',
    schedule_interval='0 0 * * *', # Daily at midnight
    catchup=False,
    tags=['example'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    hello = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> hello >> end
