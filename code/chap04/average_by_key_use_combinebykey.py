#!/usr/bin/python
#-----------------------------------------------------
# This program find average per key using 
# the combineByKey() transformation.
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

    # Combined data structure (C) is a Tuple2(sum, count)
    # 3 functions needs to be defined:
    #    v --> C
    #    C, v --> C
    #    C, C --> C
    sum_count = rdd.combineByKey(\
        lambda v: (v, 1),\
        lambda C, v: (C[0] + v, C[1] + 1),\
        lambda C1, C2: (C1[0] + C2[0], C1[1] + C2[1])\
    )

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
