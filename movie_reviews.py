import airflow
import psycopg2
import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'monica.guajardo',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 3),
    'email': ['monica.guajardo@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1),
}

FILE_NAME = 'movie_reviews.csv'
BUCKET = 'capstone-bucket-m1'
SCHEMA_NAME='blayer'
TABLE_NAME='movie_review'
CREATE_SCHEMA= f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} ;"
CREATE_TABLE= f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            cid INTEGER,
            review_str VARCHAR(255));
            """
COPY_COMMAND = f""" COPY {SCHEMA_NAME}.{TABLE_NAME} from stdin WITH CSV HEADER DELIMITER ','""" 
FILENAME = 'movie_reviews.parquet'
SQL_QUERY= "SELECT * FROM blayer.movie_reviews"
RAW_BUCKET='capstone-raw-m2'

def csv_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    cur = get_postgres_conn.cursor()
    
    with open(FILE_NAME, "r") as f:
        next(f)
        cur.copy_expert(COPY_COMMAND, file=f)
        get_postgres_conn.commit()
        cur.close()
        
with DAG (dag_id='movie_reviews_to_postgres',
          default_args=default_args,
          schedule_interval= "@once",
          catchup=False) as dag:
    
    create_movie_table = PostgresOperator(
        task_id='create_movie_review_table',
        sql= CREATE_SCHEMA + CREATE_TABLE,
        postgres_conn_id='postgres_default',
        autocommit=True)
    
    download_movie_file = GCSToLocalFilesystemOperator(
        task_id='download_movie_reviews',
        object_name=FILE_NAME,
        bucket=BUCKET,
        filename=FILE_NAME,
        gcp_conn_id= "google_cloud_default")
    
    load_movie_file = PythonOperator(
        task_id='load_csv_to_db',
        provide_context=True,
        python_callable= csv_to_postgres)
    
    upload_movie_file = PostgresToGCSOperator(
        task_id ='load_movie_reviews_to_gcs',
        sql= SQL_QUERY,
        bucket=RAW_BUCKET,
        filename=FILENAME,
        gzip=False)
    
create_movie_table >> download_movie_file >> load_movie_file >> upload_movie_file
    
    