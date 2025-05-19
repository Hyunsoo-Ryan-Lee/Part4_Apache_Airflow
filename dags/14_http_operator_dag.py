import pendulum, json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.hooks.http_hook import HttpHook


default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

http_hook = HttpHook(http_conn_id='brewery_api', method='GET')

def fetch_with_httphook():
    response = http_hook.run()

    print(pd.json_normalize(response.json()).head())
    return response.json()


def fetch_with_httpoperator(**kwargs):
    ti = kwargs.get('ti')
    
    json_data = ti.xcom_pull(key='return_value')
    
    """
    [{}, {}, {}, ....]
    
    XCOM 거치면
    
    '[{}, {}, {}, ....]'
    """
    
    # dataframe으로 변경   -> 
    data = json.loads(json_data)
    
    df = pd.json_normalize(data)
    
    # GCS에 데이터 저장!
    df.to_parquet(
        path="gs://sprintda05-airflow-hyunsoo-bucket/airflow/brewery.parquet", # 저장될 GCS의 주소!
        engine='pyarrow',
        compression='gzip',
        index=False,
        storage_options={'token':'/opt/airflow/config/sprintda05_DE_key.json'}
    )
    

with DAG(
    dag_id = '14_http_operator_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):  
    ## API 호출하여 JSON 데이터 받아옴
    get_http = HttpOperator(
        task_id='get_http',
        http_conn_id="brewery_api",
        method="GET"
    )
    
    ## 가져온 JSON 데이터를 Dataframe으로 변경 후 GCS에 저장하는 함수
    get_operator_data = PythonOperator(
        task_id='get_operator_data',
        python_callable=fetch_with_httpoperator
    )

    get_hook_data = PythonOperator(
        task_id='get_hook_data',
        python_callable=fetch_with_httphook
    )
    
    
get_http >> get_operator_data >> get_hook_data