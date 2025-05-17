import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def print_msg():
    print("THIS IS AIRFLOW!")

with DAG(
    dag_id = '01_python_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    """
    PythonOperator -> 파이썬 함수를 airflow에서 실행시켜주는 오퍼레이터!
    """
    PythonOperator(
        task_id = "python_task1",
        python_callable=print_msg
    )