from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load CSV into DataFrame") \
    .getOrCreate()
# Load CSV file into DataFrame
df = spark.read.option("header", "true").csv("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv")

# Show the DataFrame schema and some sample data
df.printSchema()
df.show(5)

