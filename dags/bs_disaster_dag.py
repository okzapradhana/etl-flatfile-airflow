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
BIGQUERY_TABLE_NAME = "bs_disaster"
GCS_OBJECT_NAME = "extract_disaster_data.csv"
DATA_PATH = f"{BASE_PATH}/data"
OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"

@dag(
    default_args={
        'owner': 'okza',
        'email': 'datokza@gmail.com',
        'email_on_failure': True
    },
    schedule_interval='0 4 * * * ',  # every 4AM
    start_date=days_ago(1),
    tags=['csv', 'disaster', 'blank-space']
) 
def bs_disaster_dag():
    @task()
    def extract_transform():
      df = pd.read_csv(f"{DATA_PATH}/disaster_data.csv")
      columns = ['text', 'location']
      for column in columns:
        df[column] = df[column].str.replace(r'\s{2,}', ' ', regex=True)
        df[column] = df[column].str.replace(r"[^a-zA-Z0-9\,]", ' ', regex=True)

      df.to_csv(OUT_PATH, index=False, header=False)
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extract_transform_task = extract_transform()

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
            {'name': 'keyword', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'target', 'type': 'INT64', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    start >> extract_transform_task
    extract_transform_task >> stored_data_gcs
    stored_data_gcs >> loaded_data_bigquery
    loaded_data_bigquery >> end

bs_disaster_elt = bs_disaster_dag()