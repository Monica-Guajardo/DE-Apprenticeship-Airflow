import airflow
import psycopg2
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from datetime import timedelta
from datetime import datetime

GoogleDriveToGCSOperator.template_fields = (
        "bucket_name",
        "object_name",
        "folder_id",
        "file_name",
        "drive_id",
        "impersonation_chain",
    )

default_args = {
    'owner': 'monica.guajardo',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 3),
    'email': ['monica.guajardo@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2),
}

FILE_NAME = 'user_purchase.csv'
BUCKET = 'capstone-bucket-m1'
SCHEMA_NAME='blayer'
TABLE_NAME='user_purchase'


def csv_to_postgres():
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    cur = get_postgres_conn.cursor()
    
    with open(FILE_NAME, "r") as f:
        next(f)
        cur.copy_from(f, "user_purchase", sep =",")
        get_postgres_conn.commit()
        cur.close()

with DAG (dag_id='upload_data_postgres',
          default_args=default_args,
          schedule_interval= "@once",
          catchup=False) as dag:
    
    create_postgres_table = PostgresOperator(
        task_id='create_table',
        sql= f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(20)); 
            """,
        postgres_conn_id='postgres_default',
        autocommit=True,
    )
    
    download_file = GCSToLocalFilesystemOperator(
        task_id='download_file',
        object_name= FILE_NAME,
        bucket= BUCKET,
        filename= FILE_NAME,
        gcp_conn_id="google_cloud_default"  
    )
    
    load_csv_to_postgres = PythonOperator(
        task_id='load_csv_to_postgres',
        provide_context=True,
        python_callable = csv_to_postgres
    )
    
    
create_postgres_table >> download_file >> load_csv_to_postgres