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
BIGQUERY_TWEETS_TABLE_NAME = "bs_tweets"
BIGQUERY_TWEETS_USER_TABLE_NAME = "bs_tweets_user"
GCS_OBJECT_TWEETS_NAME = "extract_transform_tweets.csv"
GCS_OBJECT_TWEETS_USER_NAME = "extract_transform_tweets_user.csv"
DATA_PATH =f"{BASE_PATH}/data"
OUT_TWEETS_PATH = f"{DATA_PATH}/{GCS_OBJECT_TWEETS_NAME}"
OUT_TWEETS_USER_PATH = f"{DATA_PATH}/{GCS_OBJECT_TWEETS_USER_NAME}"

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
def bs_tweets_dag():
    @task()
    def extract_transform_tweets():
      df = pd.read_json(f"{DATA_PATH}/tweet_data.json", lines=True)
      df['created_at'] = df['created_at'].dt.tz_convert(None)
      columns = ['text', 'source']
      for column in columns:
        df[column] = df[column].str.replace(r"[^a-zA-Z0-9\,#@]", ' ', regex=True)
        df[column] = df[column].str.replace(r"\s{2,}", ' ', regex=True)

      filtered_columns = filter(lambda col: 
                              col != 'extended_entities' and
                              col != 'contributors' and col != 'entities' 
                              and col != 'retweeted_status' and col != 'user'
                              and col != 'in_reply_to_user_id_str' 
                              and col != 'in_reply_to_status_id_str' ,
                            list(df.columns))
      df_filtered = df[filtered_columns]
      df_filtered.to_csv(OUT_TWEETS_PATH, index=False, header=False)
      
    @task()
    def extract_transform_tweets_user():
      df = pd.read_json(f"{DATA_PATH}/tweet_data.json", lines=True)
      users = [ {**row['user'], 'tweet_id': row['id']} for _, row in df.iterrows() ]

      df_users = pd.DataFrame(users)
      df_users['created_at'] = pd.to_datetime(df_users['created_at'], 
                                              format='%a %b %d %H:%M:%S %z %Y'
                                              ).dt.tz_convert(None)
      filtered_column = list(filter(lambda col: 
                                  col != 'id' and col != 'tweet_id',
                                list(df_users.columns)))
      df_users = df_users.reindex(columns=['id', 'tweet_id', *(filtered_column)])

      df_users.to_csv(OUT_TWEETS_USER_PATH, index=False, header=False)

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    et_tweets = extract_transform_tweets()
    et_tweets_user = extract_transform_tweets_user()

    stored_tweets_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_tweets_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src=OUT_TWEETS_PATH,
        dst=GCS_OBJECT_TWEETS_NAME,
        bucket='blank-space-de-batch1-sg'
    )

    stored_tweets_user_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_tweets_user_to_gcs",
        gcp_conn_id="my_google_cloud_conn_id",
        src=OUT_TWEETS_USER_PATH,
        dst=GCS_OBJECT_TWEETS_USER_NAME,
        bucket='blank-space-de-batch1-sg'
    )

    loaded_tweets_data_bigquery = GCSToBigQueryOperator(
        task_id='load_tweets_to_bigquery',
        bigquery_conn_id='my_google_cloud_conn_id',
        bucket='blank-space-de-batch1-sg',
        source_objects=[GCS_OBJECT_TWEETS_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TWEETS_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'truncated', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'is_quote_status', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'in_reply_to_status_id', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'in_reply_to_user_id', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},            
            {'name': 'favorite_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'retweeted', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'coordinates', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'in_reply_to_screen_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'id_str', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'retweet_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'metadata', 'type': 'STRING', 'mode': 'NULLABLE'},            
            {'name': 'favorited', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'geo', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lang', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'place', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'quoted_status_id', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'quoted_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'possibly_sensitive', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'quoted_status_id_str', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    loaded_tweets_user_data_bigquery = GCSToBigQueryOperator(
        task_id='load_tweets_user_to_bigquery',
        bigquery_conn_id='my_google_cloud_conn_id',
        bucket='blank-space-de-batch1-sg',
        source_objects=[GCS_OBJECT_TWEETS_USER_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TWEETS_USER_TABLE_NAME}",
        schema_fields=[
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'tweet_id', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'follow_request_sent', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'has_extended_profile', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'profile_use_background_image', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'verified', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'translator_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_text_color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_image_url_https', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_sidebar_fill_color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'entities', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'followers_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'protected', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'default_profile_image', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'id_str', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lang', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'utc_offset', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'statuses_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'friends_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'profile_background_image_url_https', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_link_color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'following', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geo_enabled', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'profile_background_color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_banner_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_background_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'screen_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'is_translation_enabled', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'profile_background_tile', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'favourites_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'notifications', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'contributors_enabled', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'time_zone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'profile_sidebar_border_color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'default_profile', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'is_translator', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'listed_count', 'type': 'INT64', 'mode': 'NULLABLE'},           
        ], 
        autodetect=False,
        allow_quoted_newlines=True,
        write_disposition='WRITE_TRUNCATE',
    )


    start >> [et_tweets, et_tweets_user]
    et_tweets >> stored_tweets_data_gcs
    et_tweets_user >> stored_tweets_user_data_gcs
    stored_tweets_data_gcs >> loaded_tweets_data_bigquery >> end
    stored_tweets_user_data_gcs >> loaded_tweets_user_data_bigquery >> end

bs_tweets_etl = bs_tweets_dag()