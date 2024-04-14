import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min

# you may add more import if you need to



spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

# Input and output path
input_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"

output_csv = f"hdfs://ip-172-31-94-60.ec2.internal:9000/assignment2/output3/question2/"
# Get the input CSV file as a dataframe :

df_q2 = spark.read.csv(input_csv, header=True)

# Remove rows with Price Range as NULL :

df_q2 = df_q2.filter(col("Price Range").isNotNull())


# Get the best restaurant (in terms of rating) for each city for each price range :

df_best_restaurant = df_q2.groupBy("City", "Price Range").agg(max(col("Rating")).alias("Rating"))


# Get the worst restaurant (in terms of rating) for each city for each price range :

df_worst_restaurant = df_q2.groupBy("City", "Price Range").agg(min(col("Rating")).alias("Rating"))



# Join the original DataFrame with best and worst restaurants based on "Price Range", "City", and "Rating":

df_q2 = df_best_restaurant.union(df_worst_restaurant).join(df_q2, ["Price Range", "City", "Rating"], "inner")

# Reorder columns to match original order of columns :
df_q2 = df_q2.select(
    "_c0",
    "Name",
    "City",
    "Cuisine Style",
    "Ranking",
    "Rating",
    "Price Range",
    "Number of Reviews",
    "Reviews",
    "URL_TA",
    "ID_TA"
)

df_q2 = df_q2.dropDuplicates(["Price Range", "City", "Rating"])

# Reorder output to be in the desired order with cities and price range ordered in ascending order and rating in descending order :

df_q2 = df_q2.sort(col("City").asc(), col("Price Range").asc(), col("Rating").desc())


# Show the final dataframe :
print("Final df: ")
df_q2.show()

# Number of unique cities is 31 and each city has 3 price ranges. For each price range, we find best and worst restaurants. 
# Therefore, number of rows in the final dataframe should be 31 * 3 * 2 = 186

total_rows = df_q2.count()
print("Number of rows in final dataframe:", total_rows)

# Write the filtered csv to the output path :

# Write the filtered csv to the output path, coalesce to a single partition:
df_q2.coalesce(1).write.csv(output_csv, header=True)


# Stop the spark session :
spark.stop()
