#!/usr/bin/python
#-----------------------------------------------------
# This program find average per key using 
# the groupByKey() transformation.
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

    # group (key, value) pairs by key
    grouped_by_key = rdd.groupByKey()

    # show grouped_by_key
    print("grouped_by_key = ", grouped_by_key.mapValues(lambda values: list(values)).collect())
    # [
    #  ('k3', [10, 12]), 
    #  ('k2', [6, 7, 8]), 
    #  ('k1', [1, 2, 3, 4, 5])
    # ]

    # find averages 
    avg = grouped_by_key.mapValues( lambda values : float(sum(values)) / float(len(values)))
    print("avg=", avg.collect())
    # avg.collect()
    # [
    #  ('k3', 11.0), 
    #  ('k2', 7.0), 
    #  ('k1', 3.0)
    # ]
    
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
