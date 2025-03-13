from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime

# Define DAG
with DAG(
    dag_id="create_crashes_table",
    schedule_interval=None,
    start_date=datetime(2025, 2, 21),
    catchup=False,
) as dag:


    merge_data_sql = """
    MERGE INTO `chicago_traffic_crashes.crash_data` AS target
    USING `chicago_traffic_crashes.crash_data_tmp` AS source
    ON target.unique_id = source.unique_id
      
    WHEN NOT MATCHED THEN
      INSERT (unique_id,crash_record_id, crash_date, weather_condition, lighting_condition, road_defect, injuries_total, injuries_fatal, latitude, longitude)
      VALUES (source.unique_id,crash_record_id, source.crash_date, source.weather_condition, source.lighting_condition, source.road_defect, source.injuries_total, source.injuries_fatal, source.latitude, source.longitude);
    """

    create_tmp_table_sql = """
    CREATE OR REPLACE TABLE `chicago_traffic_crashes.crash_data_tmp`
    AS
    SELECT
        MD5(CONCAT(
            COALESCE(CAST(crash_record_id AS STRING), ""),
            COALESCE(CAST(crash_date AS STRING), ""),
            COALESCE(CAST(latitude AS STRING), ""),
            COALESCE(CAST(longitude AS STRING), "")
            
        )) AS unique_id,
        *
        FROM `chicago_traffic_crashes.crash_data_ext`;
    """



    create_external_bq_table_sql = """
    CREATE OR REPLACE EXTERNAL TABLE `chicago_traffic_crashes.crash_data_ext`
    (
        crash_record_id STRING OPTIONS (description=""),
        crash_date TIMESTAMP OPTIONS (description=""),
        weather_condition STRING OPTIONS (description=""),
        lighting_condition STRING OPTIONS (description=""),
        road_defect STRING OPTIONS (description=""),
        injuries_total INT64 OPTIONS (description=""),
        injuries_fatal INT64 OPTIONS (description=""),
        latitude FLOAT64 OPTIONS (description=""),
        longitude FLOAT64 OPTIONS (description="")
    )
    OPTIONS(
        format = 'PARQUET', 
        uris = ["gs://dataengineerproject-448203-bucket1/crashes/transformed_crashes/*.parquet"]
    );
    """


    # Create BigQuery table
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        dataset_id="chicago_traffic_crashes",  # Only dataset name
        table_id="crash_data",  # Only table name
        project_id="dataengineerproject-448203",
        schema_fields=[
            {"name": "unique_id", "type": "BYTES", "mode": "REQUIRED"},
            {"name": "crash_record_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "crash_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "weather_condition", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lighting_condition", "type": "STRING", "mode": "NULLABLE"},
            {"name": "road_defect", "type": "STRING", "mode": "NULLABLE"},
            {"name": "injuries_total", "type": "INT64", "mode": "NULLABLE"},
            {"name": "injuries_fatal", "type": "INT64", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
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
        task_id="merge_crash_data",
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