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
    pprint(kwargs) # pprint -> pretty print의 줄임말 / 자료형을 사람이 보기에 직관적으로 출력해주는 print 함수
    
    message = "HELLO MY NAME IS CODEIT" # 공유되어야 할 메세지!
    
    ti = kwargs.get('ti')
    
    ti.xcom_push(
        key = "my_profile_msg",
        value = message # STRING, INT, LIST, JSON ~~~
    )
    
def print_message(**kwargs):
    """
    my_profile 함수에서 생성된 메세지를 받아와 그대로 출력해주는 함수!
    """
    ti = kwargs.get('ti')
    
    message = ti.xcom_pull(
        key='my_profile_msg'
        )
    
    print(message)


with DAG(
    dag_id = '05_python_xcom_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=my_profile,
        op_kwargs=dict(
            name = 'codeit',
            age = 20,
            count = 100
            )
    )
    
    py_task2 = PythonOperator(
        task_id = "python_task2",
        python_callable=print_message
    )
    
py_task1 >> py_task2
