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
PROJECT_ID = "gcp-data-eng-appr05-596c093a"
CLUSTER_NAME= "capstone-spark-cluster-m"
REGION= "us-central1"

SPARK_JOB_MOVIES = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri":"gs://capstone-resources-m1/scripts/transform_reviews.py"} 
}
SPARK_JOB_LOGS = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri":"gs://capstone-resources-m1/scripts/transform_logs.py"} 
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "software_config" : {
        "properties" : {"spark:spark.jars.packages" : "gs://capstone-resources-m1/jar/postgresql-42.3.3.jar"}
    }
}

with DAG (dag_id='transform_data',
          default_args=default_args,
          schedule_interval= "@once",
          catchup=False) as dag:
    
#---------Create Dataproc Clusters and submit job-----------
    create_cluster = DataprocCreateClusterOperator(task_id="create_cluster",
                                                   project_id= PROJECT_ID,
                                                   cluster_config=CLUSTER_CONFIG,
                                                   region=REGION,
                                                   cluster_name=CLUSTER_NAME,
                                                   use_if_exists=True,
                                                   delete_on_error=True
                                                   )
    
    submit_movie_job = DataprocSubmitJobOperator(task_id="submit_movie_job",
                                                 job=SPARK_JOB_MOVIES,
                                                 region=REGION,
                                                 project_id=PROJECT_ID
                                                 )
    
    submit_log_job = DataprocSubmitJobOperator(task_id="submit_log_job",
                                               job=SPARK_JOB_LOGS,
                                               region=REGION,
                                               project_id=PROJECT_ID)
    
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_cluster",
                                                   project_id=PROJECT_ID,
                                                   region=REGION,
                                                   cluster_name=CLUSTER_NAME,
                                                   trigger_rule="all_success"
                                                   )
    
    
create_cluster >> submit_movie_job >> submit_log_job >> delete_cluster