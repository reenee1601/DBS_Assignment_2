

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Initialize Spark session
spark = SparkSession.builder.appName("Assignment 2 Question 1").getOrCreate()

# Input and output paths
input_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
output_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output3/question1/"

# Get the input CSV file as a dataframe :

df_q1 = spark.read.csv(input_csv, header=True)

# Filter out the dataframe to remove with no reviews :

df_q1 = df_q1.filter(col("Reviews").isNotNull() & (col("Reviews") != "[ [  ], [  ] ]")) 
# The condition (col("Reviews") != "[ [  ], [  ] ]") checks for empty reviews that have no text information within the []

# Filter out the dataframe to remove rows with rating < 1.0 :
df_q1 = df_q1.filter((col("Rating").isNotNull()) & (col("Rating") >= 1.0))

# Show the filtered dataframe :
print("Final df: ")
df_q1.show()

# Number of null or empty reviews is 0 + 9647. Number of rows with null or rating < 1.0 is 13 + 35
# Therefore, number of rows in the final dataframe should be 85562 - 9647 - 48 = 75867
total_rows = df_q1.count()
print("Number of rows in final dataframe:", total_rows)

# Write the filtered csv to the output path :

df_q1.coalesce(1).write.csv(output_csv, header=True)

# Stop the spark session :
spark.stop()
