from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # ✅ Correct import
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# List of DAGs to trigger sequentially
dag_list = ["pull_data", "pull_vehicle_data", "pull_persons_data"]

# Define the DAG
with DAG(
    dag_id="chicago_data_project",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # First trigger task
    previous_task = TriggerDagRunOperator(
        task_id=f"trigger_{dag_list[0]}",
        trigger_dag_id=dag_list[0],
        wait_for_completion=True,
    )

    # Sequentially add the remaining DAG triggers
    for dag_id in dag_list[1:]:
        current_task = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
        )
        previous_task >> current_task  # ✅ Chain sequentially
        previous_task = current_task  # Move pointer forward

    
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id='dbt_docker_dag',  
        wait_for_completion=True, 
   ) 


previous_task >> trigger_dbt



