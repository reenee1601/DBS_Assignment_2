

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Initialize Spark session
spark = SparkSession.builder.appName("Assignment 2 Question 1").getOrCreate()

# Input and output paths
input_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
output_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output/question1/"

# Get the input CSV file as a dataframe
df_q1 = spark.read.csv(input_csv, header=True)

# Filter out the dataframe to remove rows with no reviews
df_q1 = df_q1.filter(col("Reviews").isNotNull())

# Filter out the dataframe to remove rows with rating < 1.0
df_q1 = df_q1.filter((col("Rating").isNotNull()) & (col("Rating") >= 1.0))

# Show the filtered dataframe
df_q1.show()

# Write the filtered csv to the output path
df_q1.write.csv(output_csv, header=True)

# Stop the spark session
spark.stop()
