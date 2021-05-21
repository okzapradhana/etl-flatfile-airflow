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
PROJECT_ID = Variable.get("PROJECT_ID")
BIGQUERY_TABLE_NAME = "bs_customer_invoice_chinook"
OUT_PATH = "/opt/airflow/data/extract_transform_customer_invoice_chinook.csv"

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
def bs_customer_invoice_chinook_dag():
    @task()
    def extract_transform():
        conn = sqlite3.connect('/opt/airflow/data/chinook.db')
        df = pd.read_sql(f"""
                                SELECT
                                    c.CustomerId,
                                    FirstName || ' ' || LastName AS FullName,
                                    Company,
                                    Address,
                                    City,
                                    State,
                                    Country,
                                    PostalCode,
                                    Phone,
                                    Fax,
                                    Email,
                                    InvoiceId,
                                    strftime('%Y-%m-%d', InvoiceDate) AS InvoiceDate,
                                    BillingAddress,
                                    BillingCity,
                                    BillingState,
                                    BillingCountry,
                                    BillingPostalCode,
                                    Total
                                FROM customers c
                                LEFT JOIN invoices i ON c.CustomerId = i.CustomerId
                                """, conn)
        df.to_csv(OUT_PATH, index=False, header=False) #prevent on create Index column and exclude the header row

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extracted_transformed_data = extract_transform()
    start >> extracted_transformed_data

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src=OUT_PATH,
        dst='extract_transform_customer_invoice_chinook.csv',
        bucket='blank-space-de-batch1-sg'
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bigquery_conn_id='my_google_cloud_conn_id',
        bucket='blank-space-de-batch1-sg',
        source_objects=['extract_transform_customer_invoice_chinook.csv'],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'customer_id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'fax', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'invoice_id', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'invoice_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'billing_address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'total', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    extracted_transformed_data >> stored_data_gcs
    stored_data_gcs >> loaded_data_bigquery
    loaded_data_bigquery >> end

bs_customer_invoice_chinook_etl = bs_customer_invoice_chinook_dag()