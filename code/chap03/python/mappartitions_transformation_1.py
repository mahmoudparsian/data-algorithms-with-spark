from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Apply a mapPartitions() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    file(s) containing one number per record
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
"""
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

----------------------------
1. Syntax of mapPartitions()
----------------------------
Following is the syntax of PySpark mapPartitions(). 
It calls custom function f with argument as partition 
elements and performs the function and returns all 
elements of the partition. It also takes another optional 
argument preservesPartitioning to preserve the partition.

    RDD.mapPartitions(f, preservesPartitioning=False)

---------------------------
2. Usage of mapPartitions()
---------------------------
def f(single_partition):
  #perform heavy initializations like Databse connections
  for element in single_partition:
    # perform operations for element in a partition
  #end-for
  # return updated data
#end-def

target_rdd = source_rdd.mapPartitions(f)


====================
==== EXAMPLE: ======
====================
In this example, we find (N, Z, P) for all given numbers, where
  N : count of all negative numbers
  Z : count of all zero numbers
  P : count of all positive numbers

"""

#=========================================
# for testing and debugging only
def debug_a_partition(iterator):
    elements = []
    for x in iterator:
        elements.append(x)
    #end-for
    print("elements=", str(elements))
#end-def
#==========================================
# iterator : a pointer to a single partition
# [(N, Z, P) will be returned for a single partition
#
def count_NZP(iterator):
    N = 0
    Z = 0
    P = 0
    for num_as_string in iterator:
        x = int(num_as_string.strip())
        if x < 0: N += 1
        elif x > 0: P += 1
        else: Z +=1
    #end-for
    return [(N, Z, P)]
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    #========================================
    # mapPartitions() transformation
    #
    # source_rdd.mapPartitions(function) --> target_rdd
    #
    # mapPartitions() is a 1-to-1 transformation:
    # Return a new RDD by applying a function to each 
    # partition of this RDD; maps a partition into a 
    # single element of the target RDD
    #
    # mapPartitions(f, preservesPartitioning=False)
    # Return a new RDD by applying a function to each 
    # partition of this RDD.
    #
    #========================================
    input_path = sys.argv[1]
    print("input_path = ", input_path)
  
    # create an RDD with 3 partitions
    rdd = spark.sparkContext.textFile(input_path, 3)
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
    NZP_rdd = rdd.mapPartitions(count_NZP)
    print("NZP_rdd = ", NZP_rdd)
    print("NZP_rdd.count() = ", NZP_rdd.count())
    print("NZP_rdd.collect() = ", NZP_rdd.collect())

    # x: denotes (N1, Z1, P1)
    # y: denotes (N2, Z2, P2)
    final_NPZ = NZP_rdd.reduce(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
    print("final_NPZ = ", final_NPZ)


    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()

"""
sample run:

$SPARK_HOME/bin/spark-submit mappartitions_transformation_1.py sample_numbers.txt                     
input_path =  sample_numbers.txt

rdd =  sample_numbers.txt MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
rdd.count() =  32
rdd.collect() =  ['9', '5', '33', '66', '-21', '-33', '1', '2', '3', '44', '55', '66', '1', '2', '-1', '-2', '0', '5', '6', '7', '8', '0', '-8', '-9', '0', '0', '6', '7', '8', '9', '0', '-1']
rdd.getNumPartitions() =  3

elements= ['9', '5', '33', '66', '-21', '-33', '1', '2', '3', '44']
elements= ['55', '66', '1', '2', '-1', '-2', '0', '5', '6', '7', '8']
elements= ['0', '-8', '-9', '0', '0', '6', '7', '8', '9', '0', '-1']

NZP_rdd =  PythonRDD[4] at RDD at PythonRDD.scala:53
NZP_rdd.count() =  3
NZP_rdd.collect() =  [(2, 0, 8), (2, 1, 8), (3, 4, 4)]

final_NPZ =  (7, 5, 20)


"""