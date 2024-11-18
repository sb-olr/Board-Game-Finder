from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

def run_dbt():
    # subprocess.run(["dbt", "run", "--project-dir", "/path/to/your/dbt/project"], check=True)

def test_dbt():
    # subprocess.run(["dbt", "test", "--project-dir", "/path/to/your/dbt/project"], check=True)

with DAG(
    'transform_dag',
    default_args={"depends_on_past": True},
    description='Run models and tests in dbt',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=True,
) as transform_dag:

    start_transform_task = EmptyOperator(task_id='start_transform')

    wait_for_load_task = ExternalTaskSensor(
        task_id='wait_for_load',
        external_dag_id='load_dag',
        allowed_states=["success"],
        execution_date="{{ ds }}",
        poke_interval=10,
        timeout=60 * 10,
    )

    dbt_run_task = PythonOperator(
        task_id='dbt_run',
        python_callable=run_dbt,
    )
    dbt_test_task = PythonOperator(
        task_id='dbt_test',
        python_callable=test_dbt,
    )
    complete_transform_task = EmptyOperator(task_id='complete_transform')

    start_transform_task >> wait_for_load_task >> dbt_run_task >> dbt_test_task >> complete_transform_task
