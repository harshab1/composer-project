import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryGetDatasetTablesOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# importing environment variables from airflow
bigquery_dataset_id = models.Variable.get('dataset_name')
project_id = models.Variable.get('project_name')
bigquery_table_name = models.Variable.get('bigquery_table_name')
gcs_bucket_name = models.Variable.get('gcs_bucket_name')
gcs_data_path = models.variable.get('source_gcs_data_path')

# yesterday=datetime.datetime.combine(
#     datetime.datetime.today() - datetime.timedelta(1),
#     datetime.datetime.min.time())

default_dag_args={
    'start_date': datetime(2021, 7, 6),
    'email' : ['harshavardhan.bashetty@arcinsights.io'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=1),
    'project_id': project_id
}

with models.DAG(
    dag_id='composer_gcs_to_bigquery_dataloader',
    schedule_interval='@once',
    default_args=default_dag_args
) as dag:


create_dataset = BigQueryCreateEmptyDatasetOperator(
	task_id="create_dataset",
	dataset_id=dataset_id)

get_dataset_tables = BigQueryGetDatasetTablesOperator(
    task_id="get_dataset_tables", 
    dataset_id=dataset_id)

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='cloud-samples-data',
    source_objects=[source_gcs_data_path],
    destination_project_dataset_table=f"{dataset_id}.{bigquery_table_name}",
    autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1)


create_dataset >>  get_dataset_tables >> load_csv
