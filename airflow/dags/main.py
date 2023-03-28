from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import httpx
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "../../"))
from scripts.extract import Extract
from scripts.transform import Transform

dag = DAG(dag_id="projeto_agro", start_date=datetime(2023, 1, 13))

def extract(**kwargs):
    extract = Extract()
    dict_files_name=extract.save_data_raw()

    kwargs["ti"].xcom_push(
        key="dict_files_name", value=dict_files_name
    )

def transform(**kwargs):
    dict_files_name = kwargs["ti"].xcom_pull(
        key="dict_files_name", task_ids="extract_data"
    )
    transform=Transform(dict_files_name)

    transform.separate_data_faostat()
    transform.transform_columns()
    transform.clean_dataset()
    transform.merge_dataframes()
    transform.send_data_to_staging()

extract = PythonOperator(task_id="extract_data", python_callable=extract, dag=dag)
transform = PythonOperator(task_id="transform_data", python_callable=transform, dag=dag)


extract >> transform