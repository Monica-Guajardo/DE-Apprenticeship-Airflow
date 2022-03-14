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
FILENAME='user_purchase_2.csv'
BUCKET='raw-layer-m'
GOOGLE_CONN_ID='google_cloud_default'
POSTGRES_CONN_ID='postgres_default'


with DAG (dag_id='load_user_purchase_to_gcs',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False) as dag:
    
    upload_data_togcs = PostgresToGCSOperator(
        task_id='load_data_to_raw',
        postgres_conn_id='postgres_default',
        use_server_side_cursor=True,
        sql=SQL_QUERY,
        bucket=BUCKET,
        filename=FILENAME,
        export_format='csv'
    )
    ''' 
    upload_data=PostgresToGCSOperator(task_id='load_data_togcs',
                                      sql=SQL_QUERY,
                                      bucket=BUCKET,
                                      filename=FILENAME,
                                      export_format ='json',
                                      use_server_side_cursor=True,
                                      gzip=False)
    '''
    
    dummy_start=DummyOperator(task_id='test')
    dummy_end=DummyOperator(task_id='end_test')
    
    
dummy_start >> upload_data_togcs >>  dummy_end