from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#======================================
#
# NOTE: print() and collect() are used 
# fordebugging and educational purposes.
#
# @author Mahmoud Parsian
#
#======================================

# create an instance of a SparkSession as spark
spark = SparkSession.builder.getOrCreate()
print("spark.version=", spark.version)

# set input path
input_path = sys.argv[1]
print("input_path=", input_path)

# Note that the "combined data type"
# for combineByKey() is an Integer.
frequencies = spark.sparkContext.textFile(input_path)\
  .flatMap(lambda line: line.split(" "))\
  .map(lambda word: (word, 1))\
  .combineByKey(\
    lambda v: 1,\
    lambda C, v: C+1,\
    lambda C, D: C+D\
  )
#
print(frequencies.collect())

# done!
spark.stop()
