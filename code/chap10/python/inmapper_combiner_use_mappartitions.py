from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
import collections

#======================
# The inmapper_combiner_use_mappartitions.py
# program to count frequencies of unique 
# frequencies of DNA code: by using the 
# mapPartitions() transformation
#======================

# First we define a simple function, which accepts 
# a single input partition (comprised of many input 
# records) and then returns a list of (key, value) 
# pairs, where key is a character and value is an 
# aggregated frequency of that character.

# <1>
def inmapper_combiner(partition_iterator):
    # <2>
    print("partition_iterator=", partition_iterator)
    hashmap = collections.defaultdict(int)  
    # <3>
    for record in partition_iterator:  
        # <4>
        words = record.upper().split() 
        # <5>
        for word in words:  
            # <6>
            for c in word:  
                # <7>
                hashmap[c] += 1 
            #end-for
        #end-for
    #end-for
    print("hashmap=", hashmap)
    #
    
    # Python 2.x
    # <8>
    pairs = [(k, v) for k, v in hashmap.iteritems()] 

    # Python 3.x
    # pairs = [(k, v) for k, v in hashmap.items()] 
       
    print("pairs=", pairs)
    # <9>
    return  pairs 
#end-def
#======================
#<1> partition_iterator represents a single input partition comprised of a set of records
#<2> Create an empty dictionary[String, Integer]
#<3> Get a single record from a partition
#<4> Tokenize record into an array of words
#<5> Iterate over words
#<6> Iterate over each word
#<7> Aggregate characters
#<8> Flatten dictionary into a list of (character, frequency)
#<9> return the flattened list of (character, frequency)
def main():
    # <4>    
    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <file>", file=sys.stderr)
        exit(-1)

    # <5>
    spark = SparkSession\
        .builder\
        .appName("inmapper_combiner_use_mappartitions")\
        .getOrCreate()

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

    # The `mapPartitions() transformation returns a new 
    # RDD by applying a function to each input partition 
    # (as opposed to a single input record) of this RDD
    pairs = records.mapPartitions(inmapper_combiner) 
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