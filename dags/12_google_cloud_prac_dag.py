import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)
"""
아래 작업들이 수행될 수 있게 Airflow DAG 스크립트를 작성해주세요.

1. bigquery에 'airflow_prac' 데이터셋 생성 
    -> BigQueryCreateEmptyDatasetOperator

2. 'education.regdate' 테이블을 GCS의 'education/' 디렉토리 내부에 'regdate.csv' 파일로 저장 (BigQuery -> GCS)
    -> BigQueryToGCSOperator
    
3. 'sprint_pokemon.pokemon' 테이블은 type1으로 GROUY BY (type1, type_cnt) 후 'airflow_prac.pokemon_agg' 테이블로 저장 (BigQuery에서 데이터 처리 후 저장)
    -> BigQueryInsertJobOperator

4. 'airflow_prac.pokemon_agg' 테이블을 GCS의 'education/' 디렉토리 내부에 'pokemon_agg.parquet' 파일로 저장
    -> BigQueryToGCSOperator


(심화) 5. CE에 있는 'item_his.parquet' 파일을 pd.read_parquet()으로 읽어 airflow 로그에 출력해주세요.(PythonOperator)
"""
location = 'asia-northeast3'

query = """
CREATE OR REPLACE TABLE airflow_prac.pokemon_agg AS
SELECT 
    type1,
    COUNT(type1) as type_cnt
FROM sprint_pokemon.pokemon
GROUP BY type1
ORDER BY type_cnt DESC;
"""

def read_file():
    df = pd.read_parquet('/opt/airflow/config/item_his.parquet') # 도커 컨테이너 내부 경로!
    
    print(df.head(10))
    
    """
    오퍼레이터들 간에 xcom으로 데이터프레임 공유가 필요한 경우
    
    push -> df.to_json() 사용하여 JSON 형식으로 변경하여 저장
    
    pull -> 저장된 JSON 형식을 다시 데이터프레임으로 변경하여 활용!
    """
    return df.head(10).to_json()

with DAG(
    dag_id = '12_google_cloud_prac_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):  
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        gcp_conn_id='google_cloud_conn',
        dataset_id='airflow_prac',
        if_exists='ignore',
        location=location
    )
    
    bq_to_gcs_csv = BigQueryToGCSOperator(
        task_id = 'bq_to_gcs_csv',
        gcp_conn_id='google_cloud_conn',
        location=location,
        source_project_dataset_table='education.regdate',
        destination_cloud_storage_uris=['gs://sprintda05-airflow-hyunsoo-bucket/education/regdate.csv'],
        export_format='CSV', # JSON, PARQUET
        field_delimiter=',',
        print_header=True
    )
    
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
    
    bq_to_gcs_parquet = BigQueryToGCSOperator(
        task_id = 'bq_to_gcs_parquet',
        gcp_conn_id='google_cloud_conn',
        location=location,
        source_project_dataset_table='airflow_prac.pokemon_agg',
        destination_cloud_storage_uris=['gs://sprintda05-airflow-hyunsoo-bucket/education/pokemon_agg.parquet'],
        export_format='PARQUET', # JSON, PARQUET
    )
    
    read_data = PythonOperator(
        task_id='read_data',
        python_callable=read_file
    )
    
    
create_dataset >> bq_to_gcs_csv >> bigquery_job >> bq_to_gcs_parquet >> read_data