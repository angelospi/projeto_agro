import os
import sys
from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow import DAG

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "../../"))
from scripts.extract import Extract
from scripts.transform import Transform

dag = DAG(dag_id="projeto_agro", start_date=datetime(2023, 1, 13))


def extract(**kwargs):
    """
    Extract data from api
    """
    extract_class = Extract()
    dict_files_name = extract_class.save_data_raw()

    # Pass dictionary with files names to stage transform
    kwargs["ti"].xcom_push(key="dict_files_name", value=dict_files_name)


def transform(**kwargs):
    """
    Stage transform data
    """
    dict_files_name = kwargs["ti"].xcom_pull(
        key="dict_files_name", task_ids="extract_data"
    )
    transform_class = Transform(dict_files_name)

    transform_class.separate_data_faostat()
    transform_class.transform_columns()
    transform_class.clean_dataset()
    transform_class.merge_dataframes()
    transform_class.send_data_to_staging()


extract = PythonOperator(task_id="extract_data", python_callable=extract, dag=dag)
transform = PythonOperator(task_id="transform_data", python_callable=transform, dag=dag)


extract >> transform
