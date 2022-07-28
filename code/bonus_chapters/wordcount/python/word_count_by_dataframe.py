from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

#-------------------------------------------
# Word Count (detailed) using Spark Dataframe
#--------------------------------------------


# create an instance of SparkSession object
spark = SparkSession.builder.getOrCreate()


# define your input
input_path = sys.argv[1]
print("input_path=", input_path)

# read input and create a DataFrame(words: [String])
# the created df is single column table (name of column: words)
# where each row will be an array of string objects
df = spark.read\
  .text(input_path)\
  .select(F.split(F.col("value"), " ").alias("words"))

# for debugging purposes
print("df:")
df.show(100, truncate=False)
df.printSchema()

# explode each row (which is an array of string objects)
# after explosion, each row will a string object (lowercased)
words = df.select(F.explode(F.col("words")).alias("word"))\
  .select(F.lower(F.col("word")).alias("word"))

# for debugging purposes
print("words:")
words.show(100, truncate=False)
words.printSchema()

# filter: drop the words you do not want to be counted
# here we drop words if their length is less than 3
# note that words.word denote a single column value
filtered_words = words.filter(F.length(words.word) > 2)

# for debugging purposes
print("filtered_words:")
filtered_words.show(100, truncate=False)
filtered_words.printSchema()

# next group by word and find frequency
word_count = filtered_words.groupby(F.col("word")).count()

# for debugging purposes
print("word_count:")
word_count.show(100, truncate=False)
word_count.printSchema()

# filter the result of word count
# drop (word, frequency) if frequency < 2
final_word_count = word_count.where("count > 1")

# for debugging purposes
print("final_word_count:")
final_word_count.show(100, truncate=False)
final_word_count.printSchema()
