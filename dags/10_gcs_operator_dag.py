import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = '10_gcs_operator_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        gcp_conn_id='google_cloud_conn',
        bucket_name='sprintda05-airflow-hyunsoo-bucket', # 글로벌하게 유니크!
        location='asia-northeast3',
        storage_class='STANDARD'
    )
    
create_bucket