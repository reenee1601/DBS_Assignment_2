from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, size, split, col
from itertools import combinations
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Co-Cast Analysis") \
    .getOrCreate()

# Load the Parquet file into a DataFrame
df = spark.read.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part2/input/tmdb_5000_credits.parquet")

# Define a UDF to generate pairs of actors/actresses
def generate_actor_pairs(cast_list):
    pairs = combinations(cast_list, 2)
    return list(pairs)

# Register the UDF
spark.udf.register("generate_actor_pairs_udf", generate_actor_pairs, ArrayType(StructType([StructField("actor1", StringType()), StructField("actor2", StringType())])))

# Split the cast column into individual actors/actresses
df_cast = df.withColumn("actor", explode(split(col("cast"), ","))).select("movie_id", "title", "actor")

# Collect the list of actors/actresses for each movie
df_grouped = df_cast.groupBy("movie_id", "title").agg(collect_list("actor").alias("cast_list"))

# Apply the UDF to generate pairs of actors/actresses for each movie
df_pairs = df_grouped.withColumn("actor_pairs", explodeExpr("generate_actor_pairs_udf(cast_list)")) \
                     .select("movie_id", "title", "actor_pairs.actor1", "actor_pairs.actor2")

# Count the occurrences of each pair
df_count = df_pairs.groupBy("actor1", "actor2").agg(size(collect_list("movie_id")).alias("num_movies"))

# Filter out pairs that appear in at least 2 movies
df_filtered = df_count.filter(col("num_movies") >= 2)

# Save the output as Parquet files
df_filtered.write.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output/question5/")

# Stop the SparkSession
spark.stop()
