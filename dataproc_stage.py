import airflow
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator

default_args = {
    'owner': 'monica.guajardo',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 10),
    'email': ['monica.guajardo@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1),
}
SPARK_JOB = {
    
}

CLUSTER_CONFIG = {
    
}

with DAG (dag_id='transform_data',
          default_args=default_args,
          schedule_interval= "@once",
          catchup=False) as dag:
    
#---------Create Dataproc Clusters and submit job-----------
    create_cluster = DataprocCreateClusterOperator
    
    submit_movie_job = DataprocSubmitJobOperator
    
    submit_log_job = DataprocSubmitJobOperator
    
    delete_cluster = DataprocDeleteClusterOperator
    
    
create_cluster >> [submit_movie_job >> submit_log_job] >> delete_cluster