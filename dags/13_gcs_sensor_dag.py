import pendulum
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = '13_gcs_sensor_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):  
    """
    GCSObjectExistenceSensor
        - 버킷의 특정 디렉토리에 객체가 업로드 되는지 센싱!
        
    GCSToBigQueryOperator
        - 업로드 된 객체를 빅쿼리 테이블로 저장!
    """
    
    object_sensor = GCSObjectExistenceSensor(
        task_id='object_sensor',
        google_cloud_conn_id='google_cloud_conn',
        bucket='sprintda05-airflow-hyunsoo-bucket',
        object="airflow/member.parquet" # 버킷 이하 경로!       
    )
    
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        gcp_conn_id='google_cloud_conn',
        location='asia-northeast3',
        bucket='sprintda05-airflow-hyunsoo-bucket',
        source_objects=["airflow/member.parquet"],
        source_format='PARQUET',
        autodetect=True, # CSV나 JSON 파일의 경우에만 TRUE
        destination_project_dataset_table='airflow_prac.member', 
        write_disposition='WRITE_APPEND' # 같은 이름의 테이블이 있을 때 작동 설정, WRITE_TRUNCATE (replace와 동일)
    )
    
object_sensor >> gcs_to_bq