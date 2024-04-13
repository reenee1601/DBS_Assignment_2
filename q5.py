from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, size, split, col
from itertools import combinations

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Co-Cast Analysis") \
    .getOrCreate()

# Load the Parquet file into a DataFrame
df = spark.read.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part2/input/tmdb_5000_credits.parquet")

# Split the cast column into individual actors/actresses
df_cast = df.withColumn("actor", explode(split(col("cast"), ","))).select("movie_id", "title", "actor")

# Collect the list of actors/actresses for each movie
df_grouped = df_cast.groupBy("movie_id", "title").agg(collect_list("actor").alias("cast_list"))

# Generate pairs of actors/actresses for each movie
df_pairs = df_grouped.withColumn("actor_pairs", explode(combinations(col("cast_list"), 2))) \
                     .select("movie_id", "title", col("actor_pairs").alias("actors"))

# Count the occurrences of each pair
df_count = df_pairs.groupBy("actors").agg(size(collect_list("movie_id")).alias("num_movies"))

# Filter out pairs that appear in at least 2 movies
df_filtered = df_count.filter(col("num_movies") >= 2)

# Format the output as RDD
rdd_output = df_filtered.rdd.map(lambda row: (row['actors'][0], row['actors'][1], row['num_movies']))

# Save the output as Parquet files
rdd_output.saveAsTextFile("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output/question5/")

# Stop the SparkSession
spark.stop()
