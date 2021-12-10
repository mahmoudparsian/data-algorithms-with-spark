from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession

#-----------------------------------------------------
# Find Minimum and Maximum of all input by  
# using the mapPartitions() transformations.
#
# The idea is that each partition will find 
# (local_min, local_max, local_count)
# and then we find (final_min, final_max, final_count) 
# for all partitions.
# 
# input ---- partitioned ---->  partition-1, partition-2, ...
# 
# partition-1 => local1 = (local_min1, local_max1, local_count1)
# partition-2 => local2 = (local_min2, local_max2, local_count2)
# ...
#
# final_min_max = minmax(local1, local2, ...)
#
#------------------------------------------------------
# Input Parameters:
#    INPUT_PATH as a file of numbers
#
# Example: sample_numbers.txt
#
# $ cat sample_numbers.txt
#23,24,22,44,66,77,44,44,555,666
#12,4,555,66,67,68,57,55,56,45,45,45,66,77
#34,35,36,97300,78,79
#120,44,444,445,345,345,555
#11,33,34,35,36,37,47,7777,8888,6666,44,55
#10,11,44,66,77,78,79,80,90,98,99,100,102,103,104,105
#6,7,8,9,10
#8,9,10,12,12
#7777
#222,333,444,555,666,111,112,5,113,114
#5555,4444,24
#
#
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#
#
#==========================================
# Find (min, max, count) for a given single partition.
#
# partition_iterator is an iterator over 
# elements of a single partition. 
# partition_iterator : iterator over 
# set of input records and each input record
# has the format as:
# <number><,><number><,>...<number>
#
def minmax(partition_iterator):
#
    print("type(partition_iterator)=", type(partition_iterator))
    #('type(partition_iterator)=', <type 'itertools.chain'>)
    # type(partition_iterator)= <type 'generator'>
    #
    try:
        first_record = next(partition_iterator)
        print("first_record=", first_record)
    except StopIteration:
        return [(1, -1, 0)] # WHERE min > max to filter out later
#
    numbers = [int(n) for n in first_record.split(",")]
    local_min = min(numbers)
    local_max = max(numbers)
    local_count = len(numbers)
#
    # handle remaining records in a partition
    for record in partition_iterator:
        #print("record=", record)
        numbers = [int(n) for n in record.split(",")]
        min2 = min(numbers)
        max2 = max(numbers)
        # update min, max, and count
        local_count += len(numbers)
        local_max = max(local_max, max2)
        local_min = min(local_min, min2)
#   end-for
    return [(local_min, local_max, local_count)]
#end-def
#
#==========================================
#
# find final (min, max, count) from all partitions
# and filter out (1, -1, 0) tuples. Note that we
# created (1, -1, 0) from empty partitions
# min_max_count_list = [
#                       (min1, max1, count1), 
#                       (min2, max2, count2), 
#                       ...
#                      ]
#
def find_min_max_count(min_max_count_list):
    first_time = True    
    #  iterate tuple3 in min_max_count_list:
    for local_min, local_max, local_count in min_max_count_list:
        # filter out (1, -1, 0) tuples
        # to handle empty partitions
        if (local_min <= local_max):
            if (first_time):
                final_min = local_min
                final_max = local_max
                final_count = local_count
                first_time = False
            else:
                final_min = min(final_min, local_min)
                final_max = max(final_max, local_max)
                final_count += local_count
    #end-for
    return (final_min, final_max, final_count)
#end-def
#==========================================
#
def debug_partition(iterator):
    print("===begin-partition===")
    for x in iterator:
        print(x)
    print("===end-partition===")
#end-def
#
#==========================================
def main():

    if len(sys.argv) != 2:  
        print("Usage: ", __file__, "<input-path>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.appName("minmax").getOrCreate()
    #

    # handle input parameter
    input_path = sys.argv[1]
    print("input_path=", input_path)

    #=====================================
    # read input and apply mapPartitions()
    #=====================================
    rdd = spark.sparkContext.textFile(input_path)
    print("rdd=",  rdd)
    print("rdd.count=",  rdd.count())
    print("rdd.collect()=",  rdd.collect())
    print("rdd.getNumPartitions()=",  rdd.getNumPartitions())
    #
    #=====================================
    # find (min, max, count) per partition
    #=====================================     
    min_max_count = rdd.mapPartitions(minmax)
    #
    print("min_max_count=",  min_max_count)
    print("min_max_count.count=",  min_max_count.count())
    min_max_count_list = min_max_count.collect()
    print("min_max_count.collect()=",  min_max_count_list)
    
    #=====================================
    # find final (min, max, count) from all partitions
    # and filter out (1, -1, 0) tuples
    #===================================== 
    final_min, final_max, final_count = find_min_max_count(min_max_count_list)     
    print("final: (min, max, count)= (", final_min, ", ", final_max, ", ", final_count, ")")

    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
    