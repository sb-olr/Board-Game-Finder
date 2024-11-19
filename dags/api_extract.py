import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


with DAG(
    'extract_dag',
    default_args={"depends_on_past": True},
    description='Export data from API to Google Bucket',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=True,
) as export_dag:

    api_name = f"scripts/data-script.py"
    api_path = f"{AIRFLOW_HOME}/{api_name}"

    start_export_task = EmptyOperator(task_id='start_extract')

    export_task = BashOperator(
        task_id='Get_data_with_API',
        bash_command=f"poetry run python {api_path}",
    )

    complete_export_task = EmptyOperator(task_id='complete_extract')

    start_export_task >> export_task >> complete_export_task
