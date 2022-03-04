from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#======================================
#
# NOTE: print() and collect() are used for debugging and educational purposes.
#
# @author Mahmoud Parsian
#
#======================================
def main():

    # create an instance of a SparkSession as spark
    spark = SparkSession.builder.getOrCreate()

    # set input path
    input_path = sys.argv[1]
    print("input_path=", input_path)

    # create RDD from a text file
    records = spark.sparkContext.textFile(input_path)
    print(records.collect())

    words = records.flatMap(lambda line: line.split(" "))
    print(words.collect())

    pairs =  words.map(lambda word: (word, 1))
    print(pairs.collect())

    frequencies = pairs.reduceByKey(lambda a, b: a + b)
    print(frequencies.collect())

    # done!
    spark.stop()
#end-def
#======================================
if __name__ == "__main__":
    main()