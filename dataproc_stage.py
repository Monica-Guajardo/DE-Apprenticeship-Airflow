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
    create_cluster = DataprocCreateClusterOperator(task_id='create_cluster',
                                               project_id='{{ var.value.project_id }}',
                                               cluster_name='{{ var.value.dataproc_cluster_name }}',
                                               region='{{ var.value.region }}',
                                               gcp_conn_id='google_cloud_default',
                                               dag=dag)
    
    submit_movie_job = DataprocSubmitPySparkJobOperator(task_id='submit_movie_job',
                                                    main='{{ var.value.movie_job }}',
                                                    job_name='{{ var.value.movie_job_name }}',
                                                    cluster_name='{{ var.value.dataproc_cluster_name }}',
                                                    region='{{ var.value.region }}',
                                                    )
    
    submit_log_job = DataprocSubmitJobOperator(task_id='submit_log_job',
                                                    main='{{ var.value.movie_job }}',
                                                    job_name='{{ var.value.movie_job_name }}',
                                                    cluster_name='{{ var.value.dataproc_cluster_name }}',
                                                    region='{{ var.value.region }}',
                                                    )
    
    delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster',
                                               project_id='{{ var.value.project_id }}',
                                               region='{{ var.value.region }}',
                                               cluster_name='{{ var.value.dataproc_cluster_name }}',
                                               gcp_conn_id='google_cloud_default',
                                               trigger_rule='all_done',
                                               )
    
    
create_cluster >> [submit_movie_job >> submit_log_job] >> delete_cluster