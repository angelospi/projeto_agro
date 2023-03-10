from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import httpx
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "../../"))
from scripts.extract import Extract

dag = DAG(dag_id="projeto_agro", start_date=datetime(2023, 1, 13))

def extract():
    extract = Extract()
    extract.change_columns_year()
    extract.save_data_raw()

extract = PythonOperator(task_id="extract_data", python_callable=extract, dag=dag)

extract