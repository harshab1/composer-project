"""
This is DAG code to perform the following tasks:
1. Create a GCS Bucket
2. Fetch data from URL and store it in the bucket in csv format
"""

from airflow import DAG, models
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash import BashOperator
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

    """
    Data is fetched from URL and json is converted to csv then stored in the gcs bucket
    """
    bucket_name = ti.xcom_pull(task_ids='create_gcs_bucket', key='bucket_name')
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=OPTQ1LNJ9RQTYTKM'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient = "index")
    df = df.reset_index(level=0)
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    print(df.head())
    print("bucket name is: ", bucket_name)
    df.to_csv('data.csv')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob('dataset/')

    blob.upload_from_filename('data.csv')


with DAG(
    dag_id='project_dag_v6',
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


    create_bucket >> get_data 

