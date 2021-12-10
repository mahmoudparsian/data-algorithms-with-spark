from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
import collections

#=============
# The inmapper_combiner_local_aggregation.py
# program to count frequencies of unique 
# frequencies of DNA code: by using the 
# local aggregation per record level: for 
# each record, we return a dictionary[char, frequency].
# Then aggregate them for total frequency 
# per unique character.
#=============

#<1> record represents a single input record 
#<2> Create an empty dictionary[String, Integer]
#<3> Tokenize record into an array of words
#<4> Iterate over words
#<5> Iterate over each word
#<6> Aggregate characters
#<7> Flatten dictionary into a list of (character, frequency)
#<8> return the flattened list of (character, frequency)


#======================
# First we define a simple function, which accepts 
# a single input record and then returns a list of 
# (key, value) pairs, where key is a character and 
# value is an aggregated frequency of that character
# (for the given record)

# <1>
def inmapper_combiner(record):
    # <2>
    # print("record=", record)
    hashmap = collections.defaultdict(int)  
    # <3>
    words = record.upper().split() 
    # <4>
    for word in words:  
        # <5>
        for c in word:  
            # <6>
            hashmap[c] += 1 
        #end-for
    #end-for
        
    # Python 2.x
    # <7>
    pairs = [(k, v) for k, v in hashmap.iteritems()] 
    
    # Python 3.x
    # pairs = [(k, v) for k, v in hashmap.items()] 
    
    #print("pairs=", pairs)
    # <8>
    return  pairs 
#end-def
#======================
def main():
    # <4>    
    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <file>", file=sys.stderr)
        exit(-1)

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
    # RDD by applying a function to each elememt of 
    # this RDD
    pairs = records.flatMap(inmapper_combiner) 
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())
    
    frequencies = pairs.reduceByKey(lambda a, b: a+b)
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    spark.stop()
#end-def

if __name__ == '__main__':
    main()