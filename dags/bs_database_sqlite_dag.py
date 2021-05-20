import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd
import sqlite3

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models.variable import Variable
from google.cloud import storage

dataset_id = Variable.get("DATASET_ID")
project_id = Variable.get("PROJECT_ID")
bigquery_table_name = "bs_database_sqlite"
OUT_PATH = "/opt/airflow/data/extract_transform_database_sqlite.csv"

@dag(
    default_args={
        'owner': 'okza',
        'email': 'datokza@gmail.com',
        'email_on_failure': True
    },
    schedule_interval='0 4 * * * ',  # every 4AM
    start_date=days_ago(1),
    tags=['sqlite', 'blank-space']
) 
def bs_database_sqlite_etl():
    @task()
    def extract_transform():
        conn = sqlite3.connect('/opt/airflow/data/database.sqlite')
        df = pd.read_sql("""
            WITH non_dupli_reviews AS (
                SELECT 
                    * 
                FROM reviews
                GROUP BY reviewid
            ),
            
            non_dupli_content AS (
                SELECT
                    *
                FROM content
                GROUP BY reviewid
            ),
            
            non_dupli_genres AS (
                SELECT
                    reviewid,
                    GROUP_CONCAT(genre) AS concat_genre
                FROM genres
                GROUP BY reviewid
            ),
            
            non_dupli_labels AS (
                SELECT
                    reviewid,
                    GROUP_CONCAT(label) AS concat_label
                FROM labels
                GROUP BY reviewid
            ),
            
            non_dupli_years AS (
                SELECT
                    reviewid,
                    GROUP_CONCAT(year) AS concat_year
                FROM years
                GROUP BY reviewid
            )

            SELECT 
                ndr.*,
                g.concat_genre,
                l.concat_label,
                c.content,
                y.concat_year
            FROM non_dupli_reviews ndr
            LEFT JOIN non_dupli_genres g ON ndr.reviewid = g.reviewid
            LEFT JOIN non_dupli_labels l ON ndr.reviewid = l.reviewid
            LEFT JOIN non_dupli_content c ON ndr.reviewid = c.reviewid
            LEFT JOIN non_dupli_years y ON ndr.reviewid = y.reviewid
            
        """, conn)
        df.to_csv(OUT_PATH)

    @task()
    def load_to_bigquery():
        print("Load to BigQuery Process")

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extracted_transformed_data = extract_transform()
    start >> extracted_transformed_data

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src='/opt/airflow/data/disaster_data.csv',
        dst='disaster_data.csv',
        bucket='blank-space-de-batch1-sea'
    )

    extracted_transformed_data >> stored_data_gcs
    loaded = load_to_bigquery()
    stored_data_gcs >> loaded
    loaded >> end

bs_database_sqlite_dag = bs_database_sqlite_etl()
# end = DummyOperator(task_id='end')
# bs_database_sqlite_dag >> end