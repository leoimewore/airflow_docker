# airflow_docker
![image](https://github.com/user-attachments/assets/673ebfc9-766e-43fe-9bae-bde98b570d74)

# Project Objective
- Design and develop a data pipeline for batch processing the city of Chicago Traffic incidents
- Develop analytical views and dashboard with the extracted data
- Perform Data transformation with Pandas and Pyspark
- Store the traffic data into data warehouse and data lakes
- Develop data models, facts and dimension tables with dbt
- Dataset link 1: https://data.cityofchicago.org/Transportation/Traffic-Crashes-Vehicles/68nd-jvt3/about_data
- Dataset link 2: https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if/about_data
- Dataset link 3: https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d/about_data

# Technologies
- Workflow Orchestration: Apache Airflow
- Data Warehouse: Big Query
- Data Lake: Google Cloud Storage
- Data Visualization: Looker Studio
- Data Modeling: dbt
- Containerization: Docker
- Batch Processing: Spark
- Google Cloud Services: DataProc

# Process
- Leverage spark dataframe to implement schema for the pandas data.
  `from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType`

`spark = SparkSession.builder \
    .appName("Change CSV Schema") \
    .getOrCreate()`


`custom_schema = StructType([
    StructField("crash_record_id", StringType(), True),
    StructField("crash_date", TimestampType(), True),  # Specify TimestampType
    StructField("weather_condition", StringType(), True),
    StructField("lighting_condition", StringType(), True),
    StructField("road_defect", StringType(), True),
    StructField("injuries_total", IntegerType(), True),
    StructField("injuries_fatal", IntegerType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])`

`df = spark.read \
    .option("header", "true") \
    .schema(custom_schema) \
    .csv("gs://dataengineerproject-448203-bucket1/crashes/crashes.csv")`


`df.printSchema()`


`df.write \
    .mode("overwrite") \
    .parquet("gs://dataengineerproject-448203-bucket1/crashes/transformed_crashes")`

# Visualizations

![image](https://github.com/user-attachments/assets/5367a9d6-11da-4fd5-a155-10d0d648d364)
![image](https://github.com/user-attachments/assets/b1897a91-9075-40cd-ac9e-93234ce5469a)
![image](https://github.com/user-attachments/assets/e6dd3572-d04f-4d31-9753-d64e18cd7548)



