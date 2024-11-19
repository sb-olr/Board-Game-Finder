from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

def test_google_cloud_connection():
    hook = GCSHook(gcp_conn_id='google_cloud_connection')
    buckets = hook.list('bgf_game_data_bronze_layer')  # List GCS buckets to verify connection
    print("Buckets:", buckets)

with DAG(
    'test_gcs_connection',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    test_connection = PythonOperator(
        task_id='test_gcs_connection',
        python_callable=test_google_cloud_connection,
    )
