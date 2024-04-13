from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load parquet into DataFrame") \
    .getOrCreate()

df = spark.read.option("header",True)\
.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part2/input/tmdb_5000_credits.parquet")
df.printSchema()
df.show(5)