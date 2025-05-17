import pendulum
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)
# Airflow UI의 Connections 탭에서 Google Cloud 연결 정보 생성이 선행!
bigquery_hook = BigQueryHook(
    gcp_conn_id='google_cloud_conn',
    location='asia-northeast3'
).get_sqlalchemy_engine()

# Bigquery Hook을 활용해 Pandas로 테이블을 dataframe 형태로 읽어오는 함수!
def fetch_bq_data():
    df = pd.read_sql(
        sql="SELECT * FROM sprint_pokemon.pokemon",
        con=bigquery_hook # sqlalchemy conn
    )
    # 10행까지만 로그에 프린트!
    print(df.head(10))
    
    df.to_sql(
        name='sprint_pokemon.dfdf',
        con=bigquery_hook,
        if_exists='replace',
        index=False
    )
    
    return df # return_value

with DAG(
    dag_id = '08_bigquery_hook_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):
    py_task1 = PythonOperator(
        task_id = 'py_task1',
        python_callable=fetch_bq_data
    )
    
py_task1