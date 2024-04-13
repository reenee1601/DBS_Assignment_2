import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

# Input and output paths
input_csv = "/content/drive/MyDrive/Colab Notebooks/TA_restaurants_curated_cleaned.csv"
output_csv = "/content/drive/MyDrive/Colab Notebooks/assignment2/output/question1/"

# Get the input CSV file as a dataframe :

df_q1 = spark.read.csv(input_csv, header=True)

# Filter out the dataframe to remove with no reviews :

df_q1 = df_q1.filter(col("Reviews").isNotNull())

# Filter out the dataframe to remove rows with rating < 1.0 :
df_q1 = df_q1.filter((col("Rating").isNotNull()) & (col("Rating") >= 1.0))

# Show the filtered dataframe :
df_q1.show()

# Write the filtered csv to the output path :

df_q1.write.csv(output_csv, header=True)

# Stop the spark session :
spark.stop()