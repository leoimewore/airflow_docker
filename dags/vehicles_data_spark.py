from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Change CSV Schema") \
    .getOrCreate()

# Define the schema

custom_schema = StructType([
    StructField('crash_unit_id', StringType(), True), 
    StructField('crash_record_id', StringType(), True), 
    StructField('crash_date', TimestampType(), True), 
    StructField('unit_no', StringType(), True), 
    StructField('unit_type', StringType(), True), 
    StructField('vehicle_id', IntegerType(), True),  
    StructField('make', StringType(), True), 
    StructField('model', StringType(), True), 
])

# Read CSV file with custom schema
df = spark.read \
    .option("header", "true") \
    .schema(custom_schema) \
    .csv("gs://dataengineerproject-448203-bucket1/crashes/vehicles.csv")

# Show the schema of the DataFrame to confirm the types
df.printSchema()

# Show a sample of the data
df.show()

# Optional: Write the DataFrame to a new file (e.g., Parquet)
df.write \
    .mode("overwrite") \
    .parquet("gs://dataengineerproject-448203-bucket1/crashes/transformed_vehicles")