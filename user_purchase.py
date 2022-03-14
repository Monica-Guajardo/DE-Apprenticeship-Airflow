import airflow
import psycopg2
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
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

FILE_NAME = 'user_purchase.csv'
SOURCE_BUCKET ='capstone-resources-m1'
BUCKET = 'raw-layer-m'
SCHEMA_NAME='blayer'
TABLE_NAME='user_purchase'
OBJECT= ['user_purchase.csv', 'movie_reviews.csv','log_reviews.csv']
CREATE_SCHEMA= f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} ;"
CREATE_TABLE=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(20)); 
            """
COPY_COMMAND = f""" COPY {SCHEMA_NAME}.{TABLE_NAME} from stdin WITH CSV HEADER DELIMITER ','"""        

def csv_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    cur = get_postgres_conn.cursor()
    
    with open(FILE_NAME, "r") as f:
        next(f)
        cur.copy_expert(COPY_COMMAND, file=f)
        get_postgres_conn.commit()
    cur.close()

def remove_local():
    os.remove(FILE_NAME)

with DAG (dag_id='create_postgres_instance',
          default_args=default_args,
          schedule_interval= "@once",
          catchup=False) as dag:
  
#--------Fetch "external" resources and place them on raw layer-------#  
    get_user_resources= GCSToGCSOperator(
        task_id='get_users_from_source',
        source_bucket=SOURCE_BUCKET,
        source_object= OBJECT[1],
        destination_bucket=BUCKET,
        destination_object=OBJECT[1],
        gcp_conn_id="google_cloud_default"
    ) 
    get_review_resources= GCSToGCSOperator(
        task_id='get_reviews_from_source',
        source_bucket=SOURCE_BUCKET,
        source_object= OBJECT[2],
        destination_bucket=BUCKET,
        destination_object=OBJECT[2],
        gcp_conn_id="google_cloud_default"
    ) 
    get_log_resources= GCSToGCSOperator(
        task_id='get_logs_from_source',
        source_bucket=SOURCE_BUCKET,
        source_object= OBJECT[0],
        destination_bucket=BUCKET,
        destination_object=OBJECT[0],
        gcp_conn_id="google_cloud_default"
    ) 
#----------Create scheme and Table-----------#
    create_schema = PostgresOperator(
        task_id='create_schema_for_table',
        sql = CREATE_SCHEMA,
        postgres_conn_id='postgres_default',
        autocommit=True)
    
    create_postgres_table = PostgresOperator(
        task_id='create_table',
        sql=CREATE_TABLE,
        postgres_conn_id= 'postgres_default',
        autocommit=True)
#-------Download file to  local and load to DB--------#   
    download_file = GCSToLocalFilesystemOperator(
        task_id='download_file',
        object_name= FILE_NAME,
        bucket= BUCKET,
        filename= FILE_NAME,
        gcp_conn_id="google_cloud_default")
    
    load_csv_to_postgres = PythonOperator(
        task_id='load_csv_to_postgres',
        provide_context=True,
        python_callable = csv_to_postgres)
    
    remove_local = PythonOperator(
        task_id='remove_file_from_local',
        provide_context=True,
        python_callable = remove_local
    )
    
    
(get_resources, get_review_resources, get_log_resources) >> create_schema >> create_postgres_table >> download_file >> load_csv_to_postgres >> remove_local