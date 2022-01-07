#!/usr/bin/python
#-----------------------------------------------------
# This program find median per key using
# the reduceByKey() transformation.
#
# To find median(values), we use Python's statistics package:
#
# >>> # Import statistics Library
# >>> import statistics
# >>>
# >>> # Calculate middle values
# >>> print(statistics.median([1, 3, 5, 7, 9, 11, 13]))
# 7
# >>> print(statistics.median([1, 3, 5, 7, 9, 11]))
# 6.0
# >>> print(statistics.median([-11, 5.5, -3.4, 7.1, -9, 22]))
# 1.05
#------------------------------------------------------
# Note-1: print() and collect() are used for debugging and educational purposes only.
#
#------------------------------------------------------
# Input Parameters:
#    none
#-------------------------------------------------------
#
# @author Mahmoud Parsian
#-------------------------------------------------------
#
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
import statistics

def main():
    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    input = [("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),\
             ("k2", 1), ("k2", 2), ("k2", 6), ("k2", 7), ("k2", 8),\
             ("k3", 10), ("k3", 12), ("k3", 30), ("k3", 32)]

    # build RDD<key, value>
    rdd = spark.sparkContext.parallelize(input)

    # convert each v into [v]
    # rdd: RDD[(String, Integer)]
    # list_rdd: RDD[(String, [Integer])]
    list_rdd = rdd.mapValues(lambda v: [v])

    # reduce values by key
    # list_rdd: RDD[(String, [Integer])]
    # reduced_by_key: RDD[(String, [Integer])]
    reduced_by_key = list_rdd.reduceByKey(lambda a, b : a+b)

    # show reduced_by_key
    print("reduced_by_key = ", reduced_by_key.collect())
    # [
    #  ('k3', [10, 12, 30, 32]),
    #  ('k2', [6, 7, 8, 1, 2]),
    #  ('k1', [1, 2, 3, 4, 5])
    # ]

    # find median per key 
    median_per_key = reduced_by_key.mapValues(statistics.median)
    print("median_per_key=", median_per_key.collect())
    # avg.collect()
    # [
    #  ('k3', 21.0),
    #  ('k2', 6.0),
    #  ('k1', 3.0)
    # ]
    
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()


"""

sample run:

$SPARK_HOME/bin/spark-submit exact_median_by_key_use_reducebykey.py
reduced_by_key =  
[
 ('k1', [1, 2, 3, 4, 5]), 
 ('k3', [10, 12, 30, 32]), 
 ('k2', [1, 2, 6, 7, 8])
]
median_per_key= 
[
 ('k1', 3), 
 ('k3', 21.0), 
 ('k2', 6)
]

"""