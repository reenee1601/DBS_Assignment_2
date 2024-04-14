import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit

# Input and output path
input_path = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"

output_path = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output3/question3/"


# you may add more import if you need to


spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

def extract_top_bottom_cities(input_path, output_path):
    # Read CSV file into DataFrame
    df = spark.read.csv(input_path, header=True)

    # Calculate average rating per city
    avg_rating_per_city = df.groupBy("City").agg(avg(col("Rating")).alias("AverageRating"))

    # Identify top and bottom cities
    top_cities = avg_rating_per_city.orderBy(col("AverageRating").desc()).limit(3)
    bottom_cities = avg_rating_per_city.orderBy(col("AverageRating")).limit(3)

    # Add RatingGroup column
    top_cities = top_cities.withColumn("RatingGroup", lit("Top"))
    bottom_cities = bottom_cities.withColumn("RatingGroup", lit("Bottom"))

    # Combine top and bottom cities
    combined_cities = top_cities.union(bottom_cities)

    # Show the combined DataFrame with the desired columns
    combined_cities.select("City", "AverageRating", "RatingGroup").show()

    # Write the combined DataFrame to output path
    combined_cities.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    # Call the function to extract top and bottom cities by average rating
    extract_top_bottom_cities(input_path, output_path)

    # Stop SparkSession
    spark.stop()
