from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Change CSV Schema") \
    .getOrCreate()

# Define the schema
custom_schema = StructType([
    StructField("crash_record_id", StringType(), True),
    StructField("crash_date", TimestampType(), True),  # Specify TimestampType
    StructField("weather_condition", StringType(), True),
    StructField("lighting_condition", StringType(), True),
    StructField("road_defect", StringType(), True),
    StructField("injuries_total", IntegerType(), True),
    StructField("injuries_fatal", IntegerType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])

# Read CSV file with custom schema
df = spark.read \
    .option("header", "true") \
    .schema(custom_schema) \
    .csv("gs://dataengineerproject-448203-bucket1/crashes/crashes.csv")

# Show the schema of the DataFrame to confirm the types
df.printSchema()

# Show a sample of the data
df.show()

# Optional: Write the DataFrame to a new file (e.g., Parquet)
df.write \
    .mode("overwrite") \
    .parquet("gs://dataengineerproject-448203-bucket1/crashes/transformed_crashes")
