from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests
import pandas as pd

default_args = {
    'owner':'airflow',
    'start_date':datetime(2023,5,31),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

def extract(ti):
    url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-line-lists'
    req = requests.request('GET' ,url)
    req.encoding = "utf-8"
    data = req.json()
    df = pd.DataFrame(data)
    ti.xcom_push(key='extract', value=df)


def transform(ti):
    df = ti.xcom_pull(task_ids='extract_data',key='extract')
    df.drop('job', axis=1, inplace= True)
    df.dropna(inplace=True)
    df['age_number'] = df['age_number'].astype('int')
    df['age_range'] = df['age_range'].str.replace(r'[^\d><=-]',' ',regex=True)
    df['age_range'] = df['age_range'].str.replace('< 10','1-9',regex=True)
    df['age_range'] = df['age_range'].str.replace('>= 70','70-100',regex=True)
    ti.xcom_push(key='transform', value=df)


def creat_table():
    mysql_hook = MySqlHook(mysql_conn_id='mysql')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    query = """
        CREATE TABLE IF NOT EXISTS covid19(
        id int PRIMARY KEY AUTO_INCREMENT,
        year int,
        week int,
        gender varchar(225),
        age int,
        age_range varchar(225),
        risk varchar(225),
        patient_type varchar(225),
        reporting_group varchar(225),
        province varchar(225),
        region_odpc varchar(225),
        region varchar(225),
        date Date
    )"""
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()



def load(ti):
    df = ti.xcom_pull(task_ids='transform',key='transform')
    mysql_hook = MySqlHook(mysql_conn_id='mysql')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    query = 'INSERT INTO covid19 (year, week, gender, age, age_range, risk, patient_type, reporting_group, province, region_odpc, region, date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for index, row in df.iterrows():
        year = row['year']
        week = row['weeknum']
        gender = row['gender']
        age = row['age_number']
        age_range = row['age_range']
        risk = row['risk']
        patient_type = row['patient_type']
        reporting_group = row['reporting_group']
        province = row['province']
        region_odpc = row['region_odpc']
        region = row['region']
        date = row['update_date']
        cursor.execute(query, (year, week, gender, age, age_range, risk, patient_type, reporting_group, province, region_odpc, region, date))

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id = 'dag_etl',
    default_args=default_args,
    schedule_interval='0 0 * * *') as dag:

    t1 = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract
    )

    t2 = PythonOperator(
        task_id = 'transform',
        python_callable = transform
    )

    t3 = PythonOperator(
        task_id = 'create_table',
        python_callable = creat_table
    )

    t4 = PythonOperator(
        task_id = 'load_data',
        python_callable = load
    )



t1 >> t2 >> t3 >> t4