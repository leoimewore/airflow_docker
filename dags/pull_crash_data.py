from airflow.models import DAG
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator

import requests
import logging
import os
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
CSV_FILE_PATH = "output_dataset.csv"
BUCKET_NAME = "dataengineerproject-448203-bucket1"
GCS_FILE_PATH ="crashes/crashes.csv"
import pandas as pd

columns=['crash_record_id', 'crash_date', 'weather_condition', 'lighting_condition','road_defect', 'injuries_total', 'injuries_fatal','latitude', 'longitude']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(offset=0, chunk_size=100000):
    url = "https://data.cityofchicago.org/resource/85ca-t3if"
    logger = logging.getLogger('airflow.task')
    seen_ids = set()  # To keep track of seen 'crash_record_id'
    first_chunk = True  # To control header writing in the CSV file

    if os.path.exists(CSV_FILE_PATH):
        os.remove(CSV_FILE_PATH)
        
    while True:
        params = {
            "$limit": chunk_size,
            "$offset": offset,
            "$$app_token": "YwIAZkmjAhaYhdDM2hIYhHOuu"
        }
        
        try:
            # Send the GET request to the Socrata API
            response = requests.get(url, params=params)
            
            # Check for request errors
            response.raise_for_status()  # This will raise an error for HTTP codes 4xx/5xx
        except requests.exceptions.RequestException as e:
            # Catch network-related errors (timeouts, connection errors, etc.)
            logger.error(f"Request failed: {e}")
            break  # Exit the loop if request fails
        except Exception as e:
            # Catch other exceptions
            logger.error(f"An unexpected error occurred: {e}")
            break  # Exit the loop if any unexpected error occurs
        
        try:
            # Attempt to parse the JSON response
            data = response.json()
            
            if not data:
                logger.info("No data returned. Exiting loop.")
                break  # No data, exit the loop
                
            df = pd.DataFrame(data)
            df= df.filter(items=columns)
            df = df.dropna()
            df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
            df["injuries_total"] = pd.to_numeric(df["injuries_total"], errors="coerce").fillna(0).astype(int)
            df["injuries_fatal"] = pd.to_numeric(df["injuries_fatal"], errors="coerce").fillna(0).astype(int)
            df["latitude"] = df["latitude"].astype(float)
            df["longitude"] = df["longitude"].astype(float)

            # Check for the 'crash_record_id' column
            if 'crash_record_id' not in df.columns:
                raise ValueError("Dataset does not contain a unique 'crash_record_id' column.")
            
            # Filter out rows where 'crash_record_id' has already been processed
            df = df[~df['crash_record_id'].isin(seen_ids)]
            seen_ids.update(df['crash_record_id'])

            # Append data to CSV file if not empty
            if not df.empty:
                df.to_csv(CSV_FILE_PATH, mode='a', index=False, header=first_chunk)
                first_chunk = False

            # Print status
            logger.info(f'{len(df)} rows added to pandas dataframe')

        except ValueError as e:
            # Handle specific ValueError (e.g., missing 'crash_record_id')
            logger.error(f"Error: {e}")
            break  # Exit the loop if dataset is invalid
        except Exception as e:
            # Catch any unexpected error while processing the data
            logger.error(f"An unexpected error occurred while processing data: {e}")
            break  # Exit the loop in case of error

        # Update the offset for the next batch
        offset += chunk_size


def trigger_child_dag(parent_dag_name, child_dag_name, args, dag):
    return TriggerDagRunOperator(
        task_id='trigger_dataproc_spark_job',
        trigger_dag_id=child_dag_name,
        dag=dag,
        wait_for_completion=True
    )
def delete_csv_file():
    if os.path.exists(CSV_FILE_PATH):
        os.remove(CSV_FILE_PATH)
        logging.info(f"Deleted file: {CSV_FILE_PATH}")
    else:
        logging.info(f"File not found: {CSV_FILE_PATH}")

        




with DAG(
    dag_id = 'pull_data',
    default_args=default_args,
    schedule_interval =None,
    start_date = datetime(2025,2,21),
    catchup = False,
) as parent_dag:


    extract_data_sodapy = PythonOperator(
        task_id= "extract_data_sodapy" ,
        python_callable= extract_data,
        
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=CSV_FILE_PATH,  # Local CSV file
        dst=GCS_FILE_PATH,  # Destination path in GCS bucket
        bucket=BUCKET_NAME,
        mime_type="text/csv",
        gcp_conn_id="google_cloud_default",  # Airflow GCP connection
        dag=parent_dag
    )

    upload_py_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_py_file_to_gcs",
        src="./dags/project.py",  # Local CSV file
        dst="crashes/sparkjob.py",  # Destination path in GCS bucket
        bucket=BUCKET_NAME,
        mime_type="text/x-python",
        gcp_conn_id="google_cloud_default",  # Airflow GCP connection
        dag=parent_dag
    )

    delete_csv_file = PythonOperator(
        task_id="delete_csv_file",
        python_callable =  delete_csv_file,
        dag = parent_dag
    )


    trigger_dataproc_spark_job = trigger_child_dag('pull_data', 'dataproc_create_submit_delete_cluster' ,default_args, parent_dag)


    # trigger_dataproc_spark_job = TriggerDagRunOperator(
    #     task_id="trigger_dataproc_spark_job",
    #     trigger_dag_id="dataproc_create_submit_delete_cluster",  # Name of the DAG to trigger
    #     conf={"message": "Create dataproc cluster, run spark job, then delete cluster"},  # Optional: Pass parameters
    #     wait_for_completion=True,  # Wait for the child DAG to finish
    # )

    trigger_create_bq_table_job = TriggerDagRunOperator(
        task_id="trigger_create_bq_table_job",
        trigger_dag_id="create_crashes_table",  # Name of the DAG to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
    )





    extract_data_sodapy >> upload_to_gcs_task >> upload_py_file_to_gcs 
    [upload_to_gcs_task, upload_py_file_to_gcs] >> delete_csv_file 
    delete_csv_file >> trigger_dataproc_spark_job >> trigger_create_bq_table_job