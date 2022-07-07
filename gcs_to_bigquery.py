import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator 
    # BigQueryGetDatasetTablesOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# importing environment variables from airflow
bigquery_dataset_id = models.Variable.get('bigquery_dataset_id')
project_id = models.Variable.get('project_id')
bigquery_table_name = models.Variable.get('bigquery_table_name')
gcs_bucket_name = models.Variable.get('gcs_bucket_name')
gcs_data_path = models.Variable.get('gcs_data_path')

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
		dataset_id=bigquery_dataset_id)

	# get_dataset_tables = BigQueryGetDatasetTablesOperator(
    # 	task_id="get_dataset_tables", 
    # 	dataset_id=bigquery_dataset_id)

	load_csv = GCSToBigQueryOperator(
    	task_id='load_csv',
    	bucket=gcs_bucket_name,
    	source_objects=[gcs_data_path],
    	destination_project_dataset_table=f"{bigquery_dataset_id}.{bigquery_table_name}",
    	autodetect=True,
    	skip_leading_rows=1)

	create_dataset >>  load_csv