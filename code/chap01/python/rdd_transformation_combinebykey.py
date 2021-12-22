from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a combineByKey() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=========================================
# The goal of this exercise is to use the
# combineByKey() transformation to find the
# (min, max, count) per key. Given a set
# of (key, number) pairs, find (min, max, count)
# per key. For example, if your input is:
# [("a", 5), ("a", 6), ("a", 7), 
#  ("b", 10), ("b", 20), ("b", 30), ("b", 40)]
# then we will create output as:
# [("a", (5, 7, 3)), ("b", (10, 40, 4))]
#
#=========================================
def create_pair(t3):
    # t3 = (name, city, number)
    name = t3[0]
    #city = t3[1]
    number = int(t3[2])
    return (name, number)
#end-def
#==========================================
# Function<V,C> createCombiner
# V : Integer
# return C as (min, max, count)
#
def createCombiner(V):
    return (V, V, 1)
#end-def
#==========================================
# Function2<C,V,C> mergeValue
# C : Tuple3 as (min, max, count)
# V : Integer
# return C as (min, max, count)
#
def mergeValue(C, V):
    return (min(C[0],V), max(C[1],V), (C[2]+1))
#end-def
#==========================================
# Function2<C,C,C> mergeCombiners
# C1 : Tuple3 as (min, max, count)
# C2 : Tuple3 as (min, max, count)
# return C as (min, max, count)
#
def mergeCombiners(C1, C2):
    return (min(C1[0],C2[0]), max(C1[1],C2[1]), (C1[2]+C2[2]))
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # combineByKey() transformation
    #
    # source_rdd.combineByKey(lambda1, lambda2, lambda3) --> target_rdd
    #
    # RDD<K,C> combineByKey(
    #                       Function<V,C> createCombiner,
    #                       Function2<C,V,C> mergeValue,
    #                       Function2<C,C,C> mergeCombiners
    #                      )
    #
    # Simplified version of combineByKey that hash-partitions the 
    # resulting RDD using the existing partitioner/parallelism level 
    # and using map-side aggregation.
    #========================================

    
    # Create a list of tuples. 
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    list_of_tuples= [('alex','Sunnyvale', 25), \
                     ('alex','Sunnyvale', 33), \
                     ('alex','Sunnyvale', 45), \
                     ('alex','Sunnyvale', 63), \
                     ('mary', 'Ames', 22), \
                     ('mary', 'Cupertino', 66), \
                     ('mary', 'Ames', 20), \
                     ('bob', 'Ames', 26)]
    print("list_of_tuples = ", list_of_tuples)
    rdd = spark.sparkContext.parallelize(list_of_tuples)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    
    #------------------------------------
    # apply a map() transformation to rdd
    # create a (key, value) pair
    #  where 
    #       key is the name (first element of tuple)
    #       value is a number
    #------------------------------------
    rdd2 = rdd.map(lambda t : create_pair(t))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())


    #------------------------------------
    # Find (min, max, count) per key:
    #
    # apply a combineByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the city name 
    #       value is (min, max, count)
    #------------------------------------
    combined = rdd2.combineByKey(createCombiner, mergeValue, mergeCombiners)
    print("combined = ", combined)
    print("combined.count() = ", combined.count())
    print("combined.collect() = ", combined.collect())
     
    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
    
