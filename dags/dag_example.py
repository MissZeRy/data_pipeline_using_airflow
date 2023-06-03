from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
    'retries' : 2,
    'retry_delay' : timedelta(minutes=2)
}

def get_hello():
    print('Hello world')

with DAG(
    dag_id = 'dag_example',
    default_args = default_args,
    description = 'test dag',
    schedule_interval = '@daily') as dag:

    t1 = BashOperator(
        task_id='task1',
        bash_command='echo "Task 1"'
    )

    t2 = BashOperator(
        task_id='task2',
        bash_command = 'echo "Task 2"'
        )
    
    t3 = PythonOperator(
        task_id='task3',
        python_callable = get_hello
        )
    
t1 >> t2 >> t3

