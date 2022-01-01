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
    
    # drop words if its length are less than WORD_LENGTH_THRESHOLD
    WORD_LENGTH_THRESHOLD = int(sys.argv[2])
    print("WORD_LENGTH_THRESHOLD=", WORD_LENGTH_THRESHOLD)

    # drop words (after reduction) if its frequency is less than FREQUENCY_THRESHOLD
    FREQUENCY_THRESHOLD = int(sys.argv[3])
    print("FREQUENCY_THRESHOLD=", FREQUENCY_THRESHOLD)


    # create RDD from a text file
    records = spark.sparkContext.textFile(input_path)
    print(records.collect())

    words = records.flatMap(lambda line: line.split(" "))
    print("words=", words.collect())

    # map side filter: filter():
    # drop words if its length are less than WORD_LENGTH_THRESHOLD
    words_filtered = words.filter(lambda word : len(word) >= WORD_LENGTH_THRESHOLD)
    print("words_filtered=", words_filtered.collect())
    
    pairs =  words_filtered.map(lambda word: (word, 1))
    print(pairs.collect())

    frequencies = pairs.reduceByKey(lambda a, b: a + b)
    print("frequencies=", frequencies.collect())
    
    # reducer side filter: filter():
    # drop words (after reduction) if its frequency is less than FREQUENCY_THRESHOLD
    filtered_frequencies = frequencies.filter(lambda x: x[1] >= FREQUENCY_THRESHOLD)
    print("filtered_frequencies=", filtered_frequencies.collect())


    # done!
    spark.stop()
#end-def
#======================================
if __name__ == "__main__":
    main()