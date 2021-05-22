import os
import sys
from airflow.operators.python import PythonOperator, get_current_context
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd
import re

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable

DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BIGQUERY_TABLE_NAME = "bs_reviews"
GCS_OBJECT_NAME = "extract_reviews.csv"
DATA_PATH =f"{BASE_PATH}/data"
OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"

@dag(
    default_args={
        'owner': 'okza',
        'email': 'datokza@gmail.com',
        'email_on_failure': True
    },
    schedule_interval='0 4 * * * ',  # every 4AM
    start_date=days_ago(1),
    tags=['excel', 'csv', 'reviews', 'blank-space']
)
def bs_reviews_dag():
    @task()
    def merge_reviews(reviews: list):
      df_merge = pd.concat([pd.read_json(review) for review in reviews], ignore_index=True)
      print(df_merge)
      df_merge.to_csv(OUT_PATH, index=False, header=False)

    @task()
    def extract_reviews(filename):
      print(filename)
      file_path = f"{DATA_PATH}/{filename}"
      if 'csv' in filename:
        df = pd.read_csv(file_path)
      else:
        df = pd.read_excel(file_path)
      print(df)
      return df.to_json()

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    filenames = os.listdir(DATA_PATH)
    filtered_filename = list(filter(lambda filename: re.match(r"(^reviews)", filename), filenames))

    extracted_list = []
    for i in range(len(filtered_filename)):
      extracted = extract_reviews(filtered_filename[i])
      extracted_list.append(extracted)

    merged = merge_reviews(extracted_list)
    start >> extracted_list >> merged

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src=OUT_PATH,
        dst=GCS_OBJECT_NAME,
        bucket='blank-space-de-batch1-sg'
    )

    merged >> stored_data_gcs

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bigquery_conn_id='my_google_cloud_conn_id',
        bucket='blank-space-de-batch1-sg',
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'listing_id', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'reviewer_id', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'reviewer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'comments', 'type': 'STRING', 'mode': 'NULLABLE'},            
        ], 
        autodetect=False,
        allow_quoted_newlines=True,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    stored_data_gcs >> loaded_data_bigquery >> end

bs_customer_invoice_chinook_etl = bs_reviews_dag()