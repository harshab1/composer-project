from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd

default_args = {
    'owner':'harshab',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)

}

def get_data(ti):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=OPTQ1LNJ9RQTYTKM'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient = "index")
    df = df.reset_index(level=0)
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    print(df.head())
    data_csv_file = df.to_csv('data.csv')
    ti.xcom_push(key='data_in_csv', value=data_csv_file)

# def upload_to_gcs(ti):
#     pass

def print_data(ti):
    data_csv_file = ti.xcom_pull(task_ids='get_data')
    df = pd.read_csv(data_csv_file)
    print(df.head())

    



with DAG(
    dag_id='project_dag_v3',
    description='Project DAG',
    default_args=default_args,
    start_date=datetime(2022,7,12,2),
    schedule_interval='@daily'
) as dag:

    get_data = PythonOperator(
        task_id = 'get_data_from_url',
        python_callable=get_data, 
    )

    print_data = PythonOperator(
        task_id = 'print_data',
        python_callable=print_data, 
    )

    get_data >> print_data



