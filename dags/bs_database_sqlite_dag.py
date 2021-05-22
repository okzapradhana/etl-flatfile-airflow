import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd
import sqlite3

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable

DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BUCKET_NAME = Variable.get("BUCKET_NAME")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BIGQUERY_TABLE_NAME = "bs_database_sqlite"
GCS_OBJECT_NAME = "extract_transform_database_sqlite.csv"
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
    tags=['sqlite', 'blank-space', 'music']
) 
def bs_database_sqlite_dag():
    @task()
    def extract_transform():
        conn = sqlite3.connect(f"{DATA_PATH}/database.sqlite")
        with open(f"{BASE_PATH}/sql/database_sqlite.sql", "r") as query:
            df = pd.read_sql(query.read(), conn)
        df.to_csv(OUT_PATH, index=False, header=False) #prevent on create Index column and exclude the header row

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extracted_transformed_data = extract_transform()

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        src=OUT_PATH,
        dst='extract_transform_database_sqlite.csv',
        bucket=BUCKET_NAME
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bigquery_conn_id=GOOGLE_CLOUD_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'reviewid', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'artist', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'score', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'best_new_music', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'author', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'author_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pub_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'pub_weekday', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'pub_day', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'pub_month', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'pub_year', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'concat_genre', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'concat_label', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'concat_year', 'type': 'STRING', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    start >> extracted_transformed_data
    extracted_transformed_data >> stored_data_gcs
    stored_data_gcs >> loaded_data_bigquery
    loaded_data_bigquery >> end

bs_database_sqlite_etl = bs_database_sqlite_dag()