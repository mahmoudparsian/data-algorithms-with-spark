from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession
#
from sortedcontainers import SortedDict

#-----------------------------------------------------
# Find Top-N per key by using mapPartitions().
# The idea is that each partition will find Top-N,
# and then we find Top-N for all partitions.
# 
# input ---- partitioned ---->  partition-1, partition-2, ...
# 
# partition-1 => local-1-Top-N
# partition-2 => local-2-Top-N
# ...
#
# final-top-N = Top-N(local-1-Top-N, local-2-Top-N, ...)
#
#------------------------------------------------------
# Input Parameters:
#    N
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
#
#==========================================
# Find Top-N for a given single partition.
#
# partition_iterator is an iterator over 
# elements of a single partition. 
# partition_iterator : iterator over 
# [(key1, number1), (key2, number2), ...]
#
def top(partition_iterator, N):
    # create a SortedDict object, but keep the size to N
    sd = SortedDict() 
    # 
    for key, number in partition_iterator: 
        # add an entry of (key, number) at sd  
        sd[number] = key
        # always keep only Top-N items
        if (len(sd) > N):
        	# remove the smallest number 
            sd.popitem(0)
            ### NOTE:
            ###    to find bottom-N, replace 
            ###    "sd.popitem(0)" with "sd.popitem(-1)"
    #end-for
    #print("local sd=", sd)
    #
    # switch key with value
    pairs = [(v, k) for k, v in sd.items()]
    #print("local top N pairs=", pairs)
    return  pairs 		
#end-def
#
#==========================================
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
        print("Usage: ", __file__, " <N>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.appName("top_N_use_mappartitions").getOrCreate()
    #
    print("spark=",  spark)

    # Top-N
    N = int(sys.argv[1])
    print("N : ", N)

    #=====================================
    # Create an RDD from a list of values
    #=====================================
    list_of_key_value = [
        ("a", 1), ("a", 7), ("a", 2), ("a", 3), 
        ("b", 2), ("b", 4),
        ("c", 10), ("c", 50), ("c", 60), ("c", 70),
        ("d", 5), ("d", 15), ("d", 25),
        ("e", 1), ("e", 2),
        ("f", 9), ("f", 2),
        ("g", 22), ("g", 12),
        ("h", 3), ("h", 4), ("h", 5), ("h", 6),
        ("i", 30), ("i", 40), 
        ("j", 50), ("j", 60),
        ("k", 30)
    ]
    #   
    print("list_of_key_value = ",  list_of_key_value)
    #
    
    #=====================================
    # create an RDD from a collection
    # Distribute a local Python collection to form an RDD
    #=====================================
    rdd = spark.sparkContext.parallelize(list_of_key_value)
    print("rdd=",  rdd)
    print("rdd.count=",  rdd.count())
    print("rdd.collect()=",  rdd.collect())
    
    #=====================================
    # Make sure same keys are combined
    # and then partition it into "num_of_partitions"
    # partitions: Here I set it to 2: (for debugging 
    # purposes):
    #=====================================
    num_of_partitions = 2
    combined = rdd.reduceByKey(lambda a, b: a+b).coalesce(num_of_partitions)
    print("combined=",  combined)
    print("combined.count=",  combined.count())
    print("combined.collect()=",  combined.collect())
    #
    
    #=====================================
    # for debugging purposes:
    # check content of each partition:
    combined.foreachPartition(debug_partition)  
    
      
    #=====================================
    # find local Top-N's per partition
    #=====================================     
    # x = (key, number)
    # x[0] --> key
    # x[1] --> number   
    topN = combined.mapPartitions(lambda partition: top(partition, N))
    print("topN = ",  topN)
    print("topN.count=",  topN.count())
    print("topN.collect()=",  topN.collect())


    #=====================================
    # find final Top-N from all partitions
    #=====================================     
    final_topN = top(topN.collect(), N)
    print("final_topN = ",  final_topN)

    # done!
    spark.stop()
#end-def 
#==========================================

if __name__ == '__main__':
    main()