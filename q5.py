from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit, from_json, col, explode, size, collect_list

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Co-Cast Analysis") \
    .getOrCreate()

# Load the Parquet file into a DataFrame
df = spark.read.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part2/input/tmdb_5000_credits.parquet")

# Extract actor names from JSON in the `cast` column
df_cast = df.withColumn("cast_list", explode(from_json(col("cast"), schema="array<struct<name:string>>"))) \
    .select("movie_id", "title", col("cast_list.name").alias("actor"))

# Generate pairs of actors/actresses for each movie (ensuring no duplicates)
df_pairs = df_cast.alias("a") \
    .join(df_cast.alias("b"),
          (col("a.movie_id") == col("b.movie_id")) & (col("a.actor") < col("b.actor")),
          "inner") \
    .select("a.movie_id", "a.title", col("a.actor").alias("actor1"), col("b.actor").alias("actor2"))

# Swap actor1 and actor2 to ensure consistent ordering
df_pairs_swapped = df_pairs.select("movie_id", "title", "actor2", "actor1").where("actor1 > actor2")

# Union the original pairs DataFrame with the swapped pairs DataFrame
df_all_pairs = df_pairs.union(df_pairs_swapped)

# Count the occurrences of each pair
df_count = df_all_pairs.groupBy("movie_id", "title", "actor1", "actor2").count()

# Filter out pairs that appear in at least 2 movies
df_filtered = df_count.filter(col("count") >= 2)

# Save the output as Parquet files with desired schema
df_filtered.select("movie_id", "title", "actor1", "actor2").coalesce(1).write.parquet(
    "hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output3/question5/",
    mode="overwrite"  # Overwrite existing output if any
)

# Stop the SparkSession
spark.stop()
