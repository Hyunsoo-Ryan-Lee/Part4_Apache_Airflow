import pendulum, random
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)
def random_language():
    lang_list = ["PYTHON", 'JAVA', 'RUST']
    lang = random.sample(lang_list, 1)
    print("SELECTED LANGUAGE : ", lang)
    return lang

def random_fruits():
    lang_list = ["APPLE", 'BANANA', 'ORANGE']
    lang = random.sample(lang_list, 1)
    print("SELECTED FRUITS : ", lang)
    return lang

with DAG(
    dag_id = '02_python_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    """
    PythonOperator -> 파이썬 함수를 airflow에서 실행시켜주는 오퍼레이터!
    """
    # random_fruits -> random_language
    
    py_task1 = PythonOperator(
        task_id = "python_task1",
        python_callable=random_language
    )
    
    py_task2 = PythonOperator(
        task_id = "python_task2",
        python_callable=random_fruits
    )
    
py_task2 >> py_task1