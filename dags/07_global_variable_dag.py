import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable



default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def print_variable():
    
    var = Variable.get('COUNTRY')

    print(var)


with DAG(
    dag_id = '07_global_variable_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=print_variable
    )
    
py_task1