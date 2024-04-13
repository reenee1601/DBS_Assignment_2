from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,lit,from_json, col, explode, size, collect_list

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

# Count the occurrences of each pair
df_count = df_pairs.groupBy(concat_ws(col("actor1"), lit(" - "), col("actor2")).alias("actors")) \
                  .agg(size(collect_list("movie_id")).alias("num_movies"))
# Filter out pairs that appear in at least 2 movies
df_filtered = df_count.filter(col("num_movies") >= 2)

# Save the output as Parquet files with desired schema
df_filtered.select("movie_id", "title", "actor1", "actor2").write.parquet("hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output/question5/")

# Stop the SparkSession
spark.stop()
