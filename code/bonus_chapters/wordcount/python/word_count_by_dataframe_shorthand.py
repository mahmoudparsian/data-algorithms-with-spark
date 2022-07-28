from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

#------------------------------------------------------
# Word Count (shorthand notation) using Spark Dataframe
#------------------------------------------------------

# create an instance of SparkSession object
spark = SparkSession.builder.getOrCreate()

# define your input
input_path = sys.argv[1]
print("input_path=", input_path)

# read input and create a DataFrame(words: [String])
# the created df is single column table (name of column: words)
# where each row will be an array of string objects
final_word_count = spark.read\
  .text(input_path)\
  .select(F.split(F.col("value"), " ").alias("words"))\
  .select(F.explode(F.col("words")).alias("word"))\
  .select(F.lower(F.col("word")).alias("word"))\
  .filter(F.length(F.col("word")) > 2)\
  .groupby(F.col("word")).count()\
  .where("count > 1")

# for debugging purposes
print("final_word_count:")
final_word_count.show(10, truncate=False)
final_word_count.printSchema()
