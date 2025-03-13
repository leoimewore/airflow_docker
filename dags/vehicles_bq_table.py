from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime

# Define DAG
with DAG(
    dag_id="create_vehicles_table",
    schedule_interval=None,
    start_date=datetime(2025, 2, 21),
    catchup=False,
) as dag:


    merge_data_sql = """
    MERGE INTO `chicago_traffic_crashes.vehicle_data` AS target
    USING `chicago_traffic_crashes.vehicle_data_tmp` AS source
    ON target.unique_id = source.unique_id
      
    WHEN NOT MATCHED THEN
      INSERT (unique_id,crash_unit_id,crash_record_id,crash_date,unit_no,unit_type,vehicle_id,make,model)
      VALUES (source.unique_id,source.crash_unit_id,source.crash_record_id,source.crash_date,source.unit_no,source.unit_type,source.vehicle_id,source.make,source.model);
    """

    create_tmp_table_sql = """
    CREATE OR REPLACE TABLE `chicago_traffic_crashes.vehicle_data_tmp`
    AS
    SELECT
        MD5(CONCAT(
            COALESCE(CAST(crash_record_id AS STRING), ""),
            COALESCE(CAST(crash_unit_id AS STRING), ""),
            COALESCE(CAST(crash_date AS STRING), ""),
            COALESCE(CAST(vehicle_id AS STRING), "")
            
        )) AS unique_id,
        *
        FROM `chicago_traffic_crashes.vehicle_data_ext`;
    """



    create_external_bq_table_sql = """
    CREATE OR REPLACE EXTERNAL TABLE `chicago_traffic_crashes.vehicle_data_ext`
    (
        crash_unit_id STRING OPTIONS (description=""),
        crash_record_id STRING OPTIONS (description=""),
        crash_date TIMESTAMP OPTIONS (description=""),
        unit_no STRING OPTIONS (description=""),
        unit_type STRING OPTIONS (description=""),
        vehicle_id INT64 OPTIONS (description=""),
        make STRING OPTIONS (description=""),
        model STRING OPTIONS (description="")
    )
    OPTIONS(
        format = 'PARQUET', 
        uris = ["gs://dataengineerproject-448203-bucket1/crashes/transformed_vehicles/*.parquet"]
    );
    """


    # Create BigQuery table
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        dataset_id="chicago_traffic_crashes",  # Only dataset name
        table_id="vehicle_data",  # Only table name
        project_id="dataengineerproject-448203",
        schema_fields=[
            {"name": "unique_id", "type": "BYTES", "mode": "REQUIRED"},
            {"name": "crash_unit_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "crash_record_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "crash_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "unit_no", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unit_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "vehicle_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "make", "type": "STRING", "mode": "NULLABLE"},
            {"name": "model", "type": "STRING", "mode": "NULLABLE"},
            
        ],

    )

  

    # Create external BigQuery table
    create_external_bq_table = BigQueryInsertJobOperator(
        task_id="create_external_bq_table",
        configuration={
            "query": {
                "query": create_external_bq_table_sql,
                "useLegacySql": False,  # Use Standard SQL
            }
        },
        project_id="dataengineerproject-448203",
    )

    create_tmp_table = BigQueryInsertJobOperator(
        task_id="create_tmp_table",
        configuration={
            "query": {
                "query": create_tmp_table_sql,
                "useLegacySql": False,  # Use Standard SQL
            }
        },
        project_id="dataengineerproject-448203",
    )

    merge_data_task = BigQueryInsertJobOperator(
        task_id="merge_vehicle_data",
        configuration={
            "query": {
                "query": merge_data_sql,
                "useLegacySql": False,  # Standard SQL
            }
        },
        project_id="dataengineerproject-448203",
    )

    # Set task dependencies
    create_bq_table >> create_external_bq_table >> create_tmp_table >> merge_data_task