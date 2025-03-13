from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Change CSV Schema") \
    .getOrCreate()

# Define the schema
columns = ['person_id', 'person_type', 'crash_record_id', 'vehicle_id','crash_date', 'sex','safety_equipment', 'airbag_deployed', 'age']

custom_schema = StructType([
    StructField('person_id', StringType(), True), 
    StructField('person_type', StringType(), True),
    StructField('crash_record_id', StringType(), True), 
    StructField('vehicle_id', StringType(), True), 
    StructField('crash_date', TimestampType(), True), 
    StructField('sex', StringType(), True), 
    StructField('safety_equipment', StringType(), True),  
    StructField('airbag_deployed', StringType(), True), 
    StructField('age', IntegerType(), True),
    
])

# Read CSV file with custom schema
df = spark.read \
    .option("header", "true") \
    .schema(custom_schema) \
    .csv("gs://dataengineerproject-448203-bucket1/crashes/persons.csv")

# Show the schema of the DataFrame to confirm the types
df.printSchema()

# Show a sample of the data
df.show()

# Optional: Write the DataFrame to a new file (e.g., Parquet)
df.write \
    .mode("overwrite") \
    .parquet("gs://dataengineerproject-448203-bucket1/crashes/transformed_persons")