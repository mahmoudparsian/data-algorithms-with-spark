from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession
 
#<1> Import the print() function
#<2> Import System-specific parameters and functions
#<3> Import SparkSession from the pyspark.sql module
#<4> Make sure that we have 2 parameters in the command line
#<5> Create an instance of a SparkSession object by using the builder pattern SparkSession.builder class
#<6> Define input path (this can be a file or a directory containing any number of files
#<7> Read input and create the first RDD as RDD[String] where each object has this foramt: "key,number"
#<8> Create (key, value) pairs RDD as (key, number)
#<9> Use aggregateByKey() to create (key, (sum, count)) per key
#<10> Apply the mapValues() transformation to find final average per key

#===================
# function: create_pair() to accept  
# a String object as "key,number" and  
# returns a (key, number) pair.
#
# record as String of "key,number"
def create_pair(record):
    tokens = record.split(",")
    # key -> tokens[0] as String
    # number -> tokens[1] as Integer
    return (tokens[0], int(tokens[1]))
# end-def
#===================
# function:  `add_pairs` accept two
# tuples of (sum1, count1) and (sum2, count2) 
# and returns sum of tuples (sum1+sum2, count1+count2).
#
# a = (sum1, count1)
# b = (sum2, count2)
def add_pairs(a, b):
    # sum = sum1+sum2
    sum = a[0] + b[0]
    # count = count1+count2 
    count = a[1] + b[1]
    return (sum, count)
# end-def
#===================
def create_spark_session():
    spark = SparkSession\
        .builder\
        .getOrCreate()
    return spark
#end-def
#===================
def main():
    # <4>    
    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <input-path>", file=sys.stderr)
        exit(-1)

    # <5>
    spark = create_spark_session()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # <6>
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # read input and create an RDD<String>
    # <7>
    records = spark.sparkContext.textFile(input_path) 
    print("records.count(): ", records.count())
    print("records.collect(): ", records.collect())

    # create a pair of (key, number) for "key,number"
    # <8>
    pairs = records.map(create_pair)
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())

    #============================================================
    # aggregateByKey(
    #    zeroValue, 
    #    seqFunc, 
    #    combFunc, 
    #    numPartitions=None, 
    #    partitionFunc=<function portable_hash at 0x7f51f1ac0668>
    # )
    #
    # Aggregate the values of each key, using given combine 
    # functions and a neutral "zero value". This function can 
    # return a different result type, U, than the type of the 
    # values in this RDD, V. Thus, we need one operation (seqFunc) 
    # for merging a V into a U and one operation (combFunc) for 
    # merging two U's, The former operation is used for merging 
    # values within a partition, and the latter is used for merging 
    # values between partitions. To avoid memory allocation, both 
    # of these functions are allowed to modify and return their 
    # first argument instead of creating a new U.
    #
    # RDD<K,U> aggregateByKey(
    #    U zero_value,
    #    Function2<U,V,U> seqFunc,
    #    Function2<U,U,U> combFunc
    # )
    #============================================================
    # aggregate the (sum, count) of each unique key
    # <9>
    # U is a pair (sum, count)
    # zero_value = (0, 0) = (local_sum, local_count)
    zero_value = (0, 0) 
    sum_count = pairs.aggregateByKey(\
        zero_value,\
        lambda U, v: (U[0]+v, U[1]+1),\
        lambda U1, U2: (U1[0]+U2[0], U1[1]+U2[1])\
    )
    #
    print("sum_count.count(): ", sum_count.count())
    print("sum_count.collect(): ", sum_count.collect())

    # create the final RDD as RDD[key, average]
    # <10>
    # v = (v[0], v[1]) = (sum, count)
    averages =  sum_count.mapValues(lambda v : float(v[0]) / float(v[1])) 
    print("averages.count(): ", averages.count())
    print("averages.collect(): ", averages.collect())

    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
