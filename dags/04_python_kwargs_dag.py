import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def my_profile(**kwargs):
    # pprint(kwargs)    
    msg = f"""
    name = {kwargs.get('name')}
    age = {kwargs.get('age')}
    count = {kwargs.get('count')}
    """
    print(msg)
    
    return msg

with DAG(
    dag_id = '04_python_kwargs_dag',
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
    
py_task1
