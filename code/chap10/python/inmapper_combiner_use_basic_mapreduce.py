from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
import collections

#======================
# The inmapper_combiner_use_basic_mapreduce.py
# program to count frequencies of unique 
# frequencies of DNA code: by using the 
# basic map() and reduce() transformations.
#======================

# First we define a simple function, which accepts 
# a single input partition (comprised of many input 
# records) and then returns a list of (key, value) 
# pairs, where key is a character and value 1.
def create_pairs(record):
    #print("record=", record)
    words = record.upper().split() 
    
    #pairs = []
    #for word in words:  
    #    for c in word:  
    #        pairs.append((c, 1)) 
    #    #end-for
    #end-for
    
    pairs = [(c, 1) for word in words for c in word]
    #print("pairs=", pairs)
    
    return  pairs 
#end-def
#======================
def main():

    # <4>    
    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <file>", file=sys.stderr)
        exit(-1)
    #end-if
    
    # <5>
    spark = SparkSession.builder.getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # <6>
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # read input and create an RDD<String>
    # Partition input into 3 chunks
    # <7>
    # records = spark.sparkContext.textFile(input_path).coalesce(3, shuffle=True)
    records = spark.sparkContext.textFile(input_path)
    print("records.count(): ", records.count())
    print("records.collect(): ", records.collect())
    print("records.getNumPartitions(): ", records.getNumPartitions())

    # The `flatMap() transformation returns a new 
    # RDD[key, value] (where key is a DNA code and 
    # value is 1) by applying a function to each 
    # element of the records RDD[String] 
    pairs = records.flatMap(create_pairs) 
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())
    
    frequencies = pairs.reduceByKey(lambda a, b: a+b)
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    spark.stop()
#end-def
#======================

if __name__ == '__main__':
    main()