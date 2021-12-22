#!/usr/bin/python
#-----------------------------------------------------
# This program find average per key using 
# the aggregateByKey() transformation.
#------------------------------------------------------
# Input Parameters:
#    none
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession\
        .builder\
        .appName("average_by_key_use_aggregatebykey")\
        .getOrCreate()
    return spark
#end-def

def main():
    # create an instance of SparkSession
    spark = create_spark_session()

    input = [("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),\
             ("k2", 6), ("k2", 7), ("k2", 8),\
             ("k3", 10), ("k3", 12)]

    # build RDD<key, value>
    rdd = spark.sparkContext.parallelize(input)

    # By KEY, simultaneously calculate the SUM (the numerator 
    # for the average that we want to compute), and COUNT (the 
    # denominator for the average that we want to compute):
    # Combined data structure (C) is a Tuple2(sum, count)
    # The following lambda functions needs to be defined:
    #
    #    zero --> C (initial-value per partition)
    #    C, v --> C (within-partition reduction)
    #    C, C --> C (cross-Partition reduction)
    #
    zero = (0.0, 0)
    # zero = (sum, count) as initial value per partition
    # C = (sum, count)
    # C1 = (sum1, count1)
    # C2 = (sum2, count2)
    # C1 + C2 = (sum1+sum2, count1+count2)
    #
    sum_count = rdd.aggregateByKey(\
        zero,\
        lambda C, V: (C[0] + V,    C[1] + 1),\
        lambda C1, C2: (C1[0] + C2[0], C1[1] + C[1]))
        
    # What did happen? The following is true about the meaning of 
    # each a and b pair above (so you can visualize what's happening):
    #
    # First lambda expression for Within-Partition Reduction Step::
    #   C: is a TUPLE that holds: (runningSum, runningCount).
    #   V: is a SCALAR that holds the next Value
    #
    # Second lambda expression for Cross-Partition Reduction Step::
    #   C1: is a TUPLE that holds: (runningSum, runningCount).
    #   C2: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).

    # Finally, calculate the average for each KEY, and collect results.
    # show sum_count
    print("sum_count = ", sum_count.collect())
    # [
    #  ('k3', (22, 2)), 
    #  ('k2', (21, 3)), 
    #  ('k1', (15, 5))
    # ]

    # find averages 
    avg = sum_count.mapValues( lambda v : float(v[0]) / float(v[1]))
    print("avg = ", avg.collect())
    # avg.collect()
    # [
    #  ('k3', 11), 
    #  ('k2', 7), 
    #  ('k1', 3)
    # ]
    
    # done!
    spark.stop()
#end-def

    
if __name__ == '__main__':
    main()

