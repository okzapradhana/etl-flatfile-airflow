import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable

DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BIGQUERY_TABLE_NAME = "bs_file1000"
GCS_OBJECT_NAME = "extract_transform_file1000.csv"
OUT_PATH = f"{BASE_PATH}/data/{GCS_OBJECT_NAME}"

@dag(
    default_args={
        'owner': 'okza',
        'email': 'datokza@gmail.com',
        'email_on_failure': True
    },
    schedule_interval='0 4 * * * ',  # every 4AM
    start_date=days_ago(1),
    tags=['excel', 'citizen', 'blank-space']
) 
def bs_file1000_dag():
    @task()
    def extract_transform():
      df = pd.read_excel(f"{BASE_PATH}/data/file_1000.xls", index_col=0).reset_index(drop=True)
      df = df.drop(columns='First Name.1')
      df['full_name'] = df['First Name'] + " " + df['Last Name']
      df['gender'] = df['Gender'].apply(lambda row: 'M' if row == 'Male' else 'F')
      df['date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
      df = df.drop(columns=['Date', 'Gender'])
      df.columns = ['first_name', 'last_name', 'country', 'age', 'id', 'full_name', 'gender', 'date']
      df = df.reindex(columns=['id','first_name','last_name','full_name','date','age','gender','country']).reset_index(drop=True)

      df.to_csv(OUT_PATH, index=False, header=False)

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extract_task = extract_transform()
    start >> extract_task

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src=OUT_PATH,
        dst=GCS_OBJECT_NAME,
        bucket='blank-space-de-batch1-sg'
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bigquery_conn_id='my_google_cloud_conn_id',
        bucket='blank-space-de-batch1-sg',
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'age', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    extract_task >> stored_data_gcs
    stored_data_gcs >> loaded_data_bigquery
    loaded_data_bigquery >> end

bs_file1000_etl = bs_file1000_dag()