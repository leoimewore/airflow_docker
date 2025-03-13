from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 13),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id='dbt_docker_dag',  # DAG name
    default_args=default_args,
    description='A DAG to run DBT using Docker',  # Description
    schedule_interval=None,  # You can set your schedule here (None means manual trigger)
    start_date=datetime(2025, 3, 12),  # Set the start date for the DAG
    catchup=False,  # Do not backfill any past runs
) as dag:

# Define the DockerOperator task
    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="leoimewore/dbt-image:v3",
        api_version="auto",
        auto_remove="success",
        command=["dbt", "build"],
        docker_url="unix://var/run/docker.sock",  # Docker daemon socket
        network_mode="bridge",
        mounts=[  # Use `mounts` instead of `volumes`
            {
                "source": "/Users/leonardisaac/airflow-docker/config/keys/my-cred.json",
                "target": "/root/.dbt/my-cred.json",
                "type": "bind",
                "read_only": True,
            }
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/root/.dbt/my-cred.json",
        },
    )

# Set the task dependencies (if needed)
run_dbt


