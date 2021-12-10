#!/usr/bin/python
#-----------------------------------------------------
# This program find average per key using 
# the reduceByKey() transformation.
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

    # map each (key, value) into (key, (value, 1))
    pairs = rdd.map(lambda kv: (kv[0], (kv[1], 1)))
    # pairs =  
    # [
    #  ("k1", (1, 1)), ("k1", (2, 1)), ("k1", (3, 1)), ("k1", (4, 1)), ("k1", (5, 1)),
    #  ("k2", (6, 1)), ("k2", (7, 1)), ("k2", (8, 1)), 
    #  ("k3", (10, 1)), ("k3", (12, 1))
    # ]
          
    # reduce by key: 
    # x = (sum1, count1)
    # y = (sum2, count2)
    # x + y --> (sum1+sum2, count1+count2)
    sum_count = pairs.reduceByKey(lambda x, y:  (x[0] + y[0], x[1] + y[1]))
    

    # show sum_count
    print("sum_count = ", sum_count.collect())
    # [
    #  ('k3', (22, 2)), 
    #  ('k2', (21, 3)), 
    #  ('k1', (15, 5))
    # ]

    # find averages 
    #     v = (sum-of-values, count-of-values)
    #     v[0] = sum-of-values
    #     v[1] = count-of-values
    #
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
