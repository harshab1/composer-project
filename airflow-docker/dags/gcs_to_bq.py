"""
This is DAG code to perfrom following tasks:
1. create a BQ dataset
2. Load data in to bq table from gcs bucket
"""

from airflow import DAG, models
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery


project_id = models.Variable.get('project_id')


default_args = {
    'owner':'harshab',
    'retries':5,
    'retry_delay':timedelta(minutes=2), 
    'project_id':project_id
}

def create_bq_dataset(ti):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Construct a full Dataset object to send to the API.
    dataset_id = str(project_id+"."+"composer_dataset_"+datetime.now().strftime("%Y%m%d_%H%M%S"))

    dataset = bigquery.Dataset(dataset_id)

    print('Dataset ID is:', dataset.dataset_id)
    print('Type of dataset id variable:', type(dataset.dataset_id))

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US-EAST1"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    print("Dataset id:",dataset.dataset_id)
    print("Type of dataset id:",type(dataset.dataset_id))
    print("---Other---")
    print("Dataset id:",dataset_id)
    print("Type of dataset id:",type(dataset_id))


    ti.xcom_push(key='dataset_id', value=dataset_id)

def load_data_to_table(ti):

    # dataset_id = ti.xcom_pull(task_ids='create_bq_dataset', key='dataset_id')
    # print("type of project id:", type(project_id))
    # print("dataset id:", dataset_id)
    # print("type of dataset id:", type(dataset_id))

    # table_id = dataset_id+"."+"stock_data"

    table_id = "arcinsights-proj1-20220706.composer_dataset_20220717_185618.stock_data"

    print("table id:", table_id)
    print("type of table id:", type(table_id))
    
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name

    # job_config = bigquery.LoadJobConfig(
    #     schema=[
    #         bigquery.SchemaField("Date", "DATE"),
    #         bigquery.SchemaField("Open", "FLOAT64"),
    #         bigquery.SchemaField("High", "FLOAT64"),
    #         bigquery.SchemaField("Low", "FLOAT64"),
    #         bigquery.SchemaField("Close", "FLOAT64"),
    #         bigquery.SchemaField("Volume", "INT64")
    #     ],
    # )

    # body = six.BytesIO(b"Washington,WA")
    # client.load_table_from_file(body, table_id, job_config=job_config).result()
    # previous_rows = client.get_table(table_id).num_rows
    # assert previous_rows > 0

    job_config = bigquery.LoadJobConfig(
        # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # schema=[
        #     bigquery.SchemaField("Date", "DATE"),
        #     bigquery.SchemaField("Open", "FLOAT64"),
        #     bigquery.SchemaField("High", "FLOAT64"),
        #     bigquery.SchemaField("Low", "FLOAT64"),
        #     bigquery.SchemaField("Close", "FLOAT64"),
        #     bigquery.SchemaField("Volume", "INT64")
        # ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect = True
    )

    uri = "gs://composer_proj_direct_bucket_20220716/stock_data.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    # destination_table = client.get_table(table_id)
    # print("Loaded {} rows.".format(destination_table.num_rows))
    

with DAG(
    dag_id = "gcs_to_bq_v19",
    description = "data_in_gcs_loaded_to_bq_table",
    default_args = default_args,
    start_date = datetime(2022,7,16),
    schedule_interval = '@once'
) as dag:

    task_1 = PythonOperator(
        task_id = "task_1",
        python_callable = create_bq_dataset
    )

    task_2 = PythonOperator(
        task_id = "task_2",
        python_callable=load_data_to_table
    )


    task_1  >> task_2