import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

query = """
CREATE OR REPLACE TABLE airflow.member AS
SELECT * FROM sprint_pokemon.member
"""
location='asia-northeast3'

with DAG(
    dag_id = '09_bigquery_operator_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        gcp_conn_id='google_cloud_conn',
        dataset_id='airflow',
        if_exists='ignore',
        location=location
    )
    
    """
    UPDATE, DELETE, INSERT, CREATE 등
    데이터 리소스의 수정이 가해지는 작업들에 적합!
    
    - 생성 후 삽입
    - 기존 데이터를 GROUP BY 후 삽입
    """
    bigquery_job = BigQueryInsertJobOperator(
        task_id='bigquery_job',
        gcp_conn_id='google_cloud_conn',
        location=location,
        configuration={
            "query": {
                "query": query,  # 실행할 SQL 쿼리
                "useLegacySql": False,  # 표준 SQL 사용
                "priority": "BATCH",
            }
        },
    )
    
create_dataset >> bigquery_job