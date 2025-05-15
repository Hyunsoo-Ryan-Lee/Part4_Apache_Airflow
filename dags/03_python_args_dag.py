import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)
def random_language(*args):
    lang = random.sample(args, 1)
    print("SELECTED LANGUAGE : ", lang)
    return lang

def random_fruits(*args):
    lang = random.sample(args, 1)
    print("SELECTED FRUITS : ", lang)
    return lang

with DAG(
    dag_id = '03_python_args_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=random_language,
        op_args=["PYTHON", 'JAVA', 'RUST', "GO", "SQL"]
    )
    
    py_task2 = PythonOperator(
        task_id = "python_task2",
        python_callable=random_fruits,
        op_args=["APPLE", 'BANANA', 'ORANGE']
    )
    
py_task2 >> py_task1
