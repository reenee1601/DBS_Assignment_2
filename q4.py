import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, split, regexp_replace

input_path = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"

output_path = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output3/question4/"


# you may add more import if you need to



spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

def analyze_restaurant_count(input_path, output_path):
    # Load the data from CSV file into a DataFrame
    df = spark.read.option("header", "true").csv(input_path)

    # Clean up Cuisine Style column
    df = df.withColumn("Cuisine Style", regexp_replace(df["Cuisine Style"], "[\[\]']", ""))

    # Split the Cuisine Style column into separate rows
    df = df.withColumn("Cuisine Style", explode(split(df["Cuisine Style"], ", ")))

    # Group by City and Cuisine Style and count the number of restaurants
    result = df.groupBy("City", "Cuisine Style").agg(count("*").alias("count")).orderBy("City", "Cuisine Style")

    # Display the result
    result.show(truncate=False)

    # Write output as CSV files into output path
    result.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    # Call the function to analyze restaurant count and write the output
    analyze_restaurant_count(input_path, output_path)

    # Stop SparkSession
    spark.stop()
