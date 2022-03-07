import airflow
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime
from datetime import timedelta

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

SQL_QUERY= "SELECT * FROM blayer.user_purchase"
FILENAME="user_purchase{}.parquet"
BUCKET='capstone-raw-layer-m1'
GOOGLE_CONN_ID='google_cloud_default'
POSTGRES_CONN_ID='postgrs_default'


with DAG (dag_id='load_user_purchase_to_gcs_parquet',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False) as dag:
    
    upload_data=PostgresToGCSOperator(task_id='load_data_toGcs',
                                      sql=SQL_QUERY,
                                      bucket=BUCKET,
                                      filename=FILENAME,
                                      
                                      gzip=False)
    
    upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id='get_data_with_server_side_cursor',
        sql=SQL_QUERY,
        bucket=BUCKET,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
        cursor_itersize= 50000,
        export_format='parquet',
        dag=dag)
    
    dummy_start=DummyOperator(task_id='test')
    dummy_end=DummyOperator(task_id='end_test')
    
    
dummy_start >> upload_data >> upload_data_server_side_cursor >> dummy_end