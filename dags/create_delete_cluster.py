from airflow import models
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule





# Define your cluster parameters
CLUSTER_NAME = 'dataproc-airflow-cluster-{{ ds_nodash }}'
REGION = 'us-central1'  # region
PROJECT_ID = 'dataengineerproject-448203'  # project name
PYSPARK_URI = 'gs://dataengineerproject-448203-bucket1/crashes/sparkjob.py'  # spark job location in cloud storage

# Cluster configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}

# PySpark job configuration
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

# Define the DAG
with models.DAG(
    dag_id='dataproc_create_submit_delete_cluster',
    schedule_interval=None,
    start_date=datetime(2025, 2, 20),
    catchup=False,
    tags=["dataproc_airflow"]
) as dag:


    # Task to create the Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # Task to submit the PySpark job
    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Task to delete the Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Set task dependencies
    create_cluster >> submit_job >> delete_cluster
