import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def my_profile(**kwargs):
    
    sss = "HELLO MY NAME IS CODEIT" # 공유되어야 할 메세지!
    iii = 10
    lll = [1,2,3]
    jjj = [{"a":1}, {'a':2}]
    
    ti = kwargs.get('ti')
    
    ti.xcom_push(
        key = "sss",
        value = sss
    )
    ti.xcom_push(
        key = "iii",
        value = iii
    )
    ti.xcom_push(
        key = "lll",
        value = lll
    )
    ti.xcom_push(
        key = "jjj",
        value = jjj
    )
    
def print_message(**kwargs):
    ti = kwargs.get('ti')
    
    sss = ti.xcom_pull(
        key='sss'
        )
    iii = ti.xcom_pull(
        key='iii'
        )
    lll = ti.xcom_pull(
        key='lll'
        )
    jjj = ti.xcom_pull(
        key='jjj'
        )
    
    print(type(sss))
    print(type(iii))
    print(type(lll))
    print(type(jjj))


with DAG(
    dag_id = '15_python_xcom_test',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=my_profile
    )
    
    py_task2 = PythonOperator(
        task_id = "python_task2",
        python_callable=print_message
    )
    
py_task1 >> py_task2
