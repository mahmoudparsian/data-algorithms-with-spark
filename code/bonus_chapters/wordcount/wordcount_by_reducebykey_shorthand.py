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

    frequencies = spark.sparkContext.textFile(input_path)\
        .flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    #
    print(frequencies.collect())

    # done!
    spark.stop()
#end-def
#======================================
if __name__ == "__main__":
    main()