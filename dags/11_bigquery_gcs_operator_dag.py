import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = '11_bigquery_gcs_operator_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):  
    """
    bigquery 테이블을 gcs에 CSV 파일로 저장하는 작업
    """
    bq_to_gcs = BigQueryToGCSOperator(
        task_id = 'bq_to_gcs',
        gcp_conn_id='google_cloud_conn',
        location='asia-northeast3',
        source_project_dataset_table='sprint_pokemon.pokemon',
        destination_cloud_storage_uris=['gs://sprintda05-airflow-hyunsoo-bucket/airflow/pokemon.csv'],
        export_format='CSV', # JSON, PARQUET
        field_delimiter=',',
        print_header=True
    )
    
bq_to_gcs
