import os
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator



with DAG(
    'load_dag',
    default_args={"depends_on_past": True},
    description='Load data from GCS to BigQuery',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=True,
) as load_dag:

    start_load_task = EmptyOperator(task_id='start_load')

    wait_for_extract_task = ExternalTaskSensor(
        task_id='wait_for_extract',
        external_dag_id='extract_dag',
        allowed_states=["success"],
        execution_date="{{ ds }}",
        poke_interval=10,
        timeout=60 * 10,
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset',
            gcp_conn_id='google_cloud_connection',
            dataset_id='bgf_project_silver',
    )
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        # bucket='',
        # source_objects=f'nameofthefile.csv',
        gcp_conn_id='google_cloud_connection',
        # destination_project_dataset_table='bgf_project_silver.something'
        # source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
    )

    complete_load_task = EmptyOperator(task_id='complete_load')


    start_load_task >> wait_for_extract_task >> create_dataset_task >> load_to_bigquery_task >> complete_load_task
