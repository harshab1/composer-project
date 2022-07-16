from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
from google.cloud import storage, bigquery

project_id = models.Variable.get('project_id')

default_args = {
    'owner':'harshab',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)

}

def create_gcs_bucket(ti):
    """
    Create a new bucket in the US-EAST1 region with the standard storage
    class
    """
    bucket_name = "composer-bucket-"+datetime.now().strftime("%Y%m%d-%H%M%S")

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us-east1")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    ti.xcom_push(key='bucket_name', value = bucket_name)

def get_data(ti):
    bucket_name = ti.xcom_pull(task_ids='create_gcs_bucket', key='bucket_name')
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=OPTQ1LNJ9RQTYTKM'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient = "index")
    df = df.reset_index(level=0)
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    print(df.head())
    data_csv_file = df.to_csv('data.csv')
    !gsutil cp data.csv gs://$bucket_name/
    # ti.xcom_push(key='data_in_csv', value=data_csv_file)

# def upload_to_gcs(ti):
#     pass

# def print_data(ti):
#     data_csv_file = ti.xcom_pull(task_ids='get_data')
#     df = pd.read_csv(data_csv_file)
#     print(df.head())


with DAG(
    dag_id='project_dag_v3',
    description='Project DAG',
    default_args=default_args,
    start_date=datetime(2022,7,12,2),
    schedule_interval='@daily'
) as dag:

    create_bucket = PythonOperator(
        task_id = 'create_gcs_bucket',
        python_callable=create_gcs_bucket, 
    )

    get_data = PythonOperator(
        task_id = 'get_data_from_url',
        python_callable=get_data, 
    )

    create_bucket_bash = BashOperator(
        task_id = 'creating_gcs_bucket_bash',
        bash_command = 'gsutil mb -p arcinsights-proj1-20220706 -c STANDARD -l US-EAST1 -b on gs://composer-sample-bucket'
    )

    # print_data = PythonOperator(
    #     task_id = 'print_data',
    #     python_callable=print_data, 
    # )

    create_bucket >> get_data >> create_bucket_bash
    
    # >> print_data



