#!/usr/bin/python
#-----------------------------------------------------
# Apply a mapPartitions() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
"""

                 Find (minimum, maximum, count) by 
                 using mapPartitions() transformation

Spark's mapPartitions()
According to Spark API: mapPartitions(func)    transformation is 
similar to map(), but runs separately on each partition (block) 
of the RDD, so func must be of type Iterator<T> => Iterator<U> 
when running on an RDD of type T.

The mapPartitions() transformation should be used when you want 
to extract some condensed information (such as finding the 
minimum and maximum of numbers) from each partition. For example, 
if you want to find the minimum and maximum of all numbers in your 
input, then using map() can be pretty inefficient, since you will 
be generating tons of intermediate (K,V) pairs, but the bottom line 
is you just want to find two numbers: the minimum and maximum of 
all numbers in your input. Another example can be if you want to 
find top-10 (or bottom-10) for your input, then mapPartitions() 
can work very well: find the top-10 (or bottom-10) per partition, 
then find the top-10 (or bottom-10) for all partitions: this way 
you are limiting emitting too many intermediate (K,V) pairs.

=================
Empty Partitions:
=================
        This example shows how to handle Empty Partitions.
        An empty partition is a partition, which has no 
        elements in it. Empty partions should be handled 
        gracefully.
"""

from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 


#=========================================
def debug_a_partition(iterator):
    print("==begin-partition=")
    for x in iterator:
        print(x)
    #end-for
    print("==end-partition=")
#end-def
#==========================================
# iterator : a pointer to a single partition
# (min, max) will be returned for a single partition
# <1> iterator is a type of 'itertools.chain'
# <2> print the type of iterator (for debugging only)
# <3> try to get the first record from a given iterator, 
# if successful, then the first_record is initialied to 
# the first record of a partition
# <4> if you are here, then it means that the partition 
# is empty, return a fake triplet
# <5> set min, max, and count from the first record
# <6> iterate the iterator for 2nd, 3rd, ... records 
# (record holds a single record)
# <7> finally return a triplet from each partition

def min_max_count(iterator):
#
#   print("type(iterator)=", type(iterator))
#   ('type(iterator)=', <type 'itertools.chain'>)
#
    try:
        first_record = next(iterator) 
    except StopIteration: 
        # WHERE min > max to filter it out later       
        return [(1, -1, 0)] 
#
    numbers = first_record.split(",")
    # convert strings to integers
    numbers = map(int, numbers)
    local_min = min(numbers)
    local_max = max(numbers)
    local_count = len(numbers)
#
    for record in iterator:
        numbers = record.split(",")  
        min2 = min(numbers)
        max2 = max(numbers)
        local_count += len(numbers)
#
        if max2 > local_max:
            local_max = max2
        if min2 < local_min:
            local_min = min2
#   end-for
    return [(local_min, local_max, local_count)]
#end-def
#==========================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: rdd_transformation_mappartitions.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_transformation_mappartitions")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #========================================
    # mapPartitions() transformation
    #
    # source_rdd.mapPartitions(function) --> target_rdd
    #
    # mapPartitions() is a 1-to-1 transformation:
    # Return a new RDD by applying a function to each partition of this RDD;
    # maps a partition into a single element of the target RDD
    #
    # mapPartitions(f, preservesPartitioning=False)[source]
    # Return a new RDD by applying a function to each partition of this RDD.
    #
    #========================================
    numbers = ["10,20,3,4",\
               "3,5,6,30,7,8",\
               "4,5,6,7,8",\
               "3,9,10,11,12",\
               "6,7,13",\
               "5,6,7,12"\
               "5,6,7,8,9,10",\
               "11,12,13,14,15,16,17"]
    #
    print("numbers = ", numbers)
  
    # create an RDD with 10 partitions
    # with high number of partitions, 
    # some of the partitions will be Empty
    rdd = spark.sparkContext.parallelize(numbers, 10)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    print("rdd.getNumPartitions() = ", rdd.getNumPartitions())
    rdd.foreachPartition(debug_a_partition)

    # Find Minimum and Maximum
    # Use mapPartitions() and find the minimum and maximum 
    # from each partition.  To make it a cleaner solution, 
    # we define a python function to return the minimum and 
    # maximum for a given iteration.
    min_max_count_rdd = rdd.mapPartitions(min_max_count)
    print("min_max_count_rdd = ", min_max_count_rdd)
    print("min_max_count_rdd.count() = ", min_max_count_rdd.count())
    print("min_max_count_rdd.collect() = ", min_max_count_rdd.collect())


    min_max_count_list = min_max_count_rdd.collect()
    print("min_max_count_list = ", min_max_count_list)
    #print("min(minmax_list) = ", min(minmax_list))
    #print("max(minmax_list) = ", max(minmax_list))

    min_max_count_filtered = min_max_count_rdd.filter(lambda x: x[0] <= x[1])
    print("min_max_count_filtered.collect() = ", min_max_count_filtered.collect())
    
    # now finalize the (min, max, count)
    min_max_count_filtered_list = min_max_count_filtered.collect()
    print("min_max_count_filtered_list = ", min_max_count_filtered_list) 
    
    #==============================
    # final final (min, max, count)
    #==============================
    # min
    # max
    # count
    first_time = 1
    for t3 in  min_max_count_filtered_list:
        if (first_time == 1):
            min = t3[0];
            max = t3[1];
            count = t3[2]
            first_time = 0
        else:
            count += t3[2]
            if t3[1] > max:
                max = t3[1]
            if t3[0] < min:
                min = t3[0]
        #end-if
    #end-for   
          
    print("final min = ", min)
    print("final max = ", max)
    print("final count = ", count)
    
    # done!
    spark.stop()

