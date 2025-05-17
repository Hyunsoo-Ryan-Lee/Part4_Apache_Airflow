import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def first_func(*args, **kwargs):
    join_list = ' '.join(args)
    
    ti = kwargs.get('ti')
    
    ti.xcom_push(
        key = "first_func_return",
        value = join_list
    )
    
def second_func(**kwargs):
    ti = kwargs.get('ti')
    
    message = ti.xcom_pull(
        key='first_func_return'
        )
    
    changed_list = '!' + message + '!'
    
    ti.xcom_push(
        key = "second_func_return",
        value = changed_list
    )
    
def third_func(**kwargs):
    ti = kwargs.get('ti')
    
    message = ti.xcom_pull(
        key='second_func_return'
        )
    
    return message.split(' ')


with DAG(
    dag_id = '06_python_xcom_prac_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=first_func,
        op_args=['FLOWER', 'AIRFLOW', 'BIGQUERY']
    )
    
    py_task2 = PythonOperator(
        task_id = "python_task2",
        python_callable=second_func
    )

    py_task3 = PythonOperator(
        task_id = "python_task3",
        python_callable=third_func
    )
    
py_task1 >> py_task2 >> py_task3
