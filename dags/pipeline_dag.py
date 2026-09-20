from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append('/usr/local/airflow/include')

from clean_data import clean_data
from load_to_snowflake import load_data

default_args = {
    'owner': 'srinidhi',
    'retries': 1,
}

with DAG(
    'support_pipeline_dag',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    def run_clean():
        clean_data(
            "/usr/local/airflow/include/superstore_raw.csv",
            "/usr/local/airflow/include/superstore_clean.csv"
        )

    def run_load():
        load_data()

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=run_clean
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=run_load
    )

    clean_task >> load_task