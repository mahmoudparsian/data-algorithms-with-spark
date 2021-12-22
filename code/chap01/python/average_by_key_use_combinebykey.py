from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a combineByKey() transformation to an 
# RDD[(key, value)] to find average per key.
# Input: NONE
# Create RDD[(key, value)] from a Collection
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=========================================
def create_pair(t3):
    # t3 = (name, city, number)
    name = t3[0]
    #city = t3[1]
    number = int(t3[2])
    # return (k, v) pair
    # v = (sum, count)
    return (name, number)
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
    # apply a combineByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the name 
    #       value is the (sum, count)
    #------------------------------------
    # v : a number for a given (key, v) pair of source RDD
    # C : is a tuple of pair (sum, count), which is
    # also called a "combined" data structure
    # C1 and C2 are a "combined" data structure:
    #   C1 = (sum1, count1)
    #   C2 = (sum2, count2)
    sum_count = rdd2.combineByKey(\
        lambda v: (v, 1),\
        lambda C, v: (C[0]+v, C[1]+1),\
        lambda C1, C2: (C1[0]+C2[0], C1[1]+C2[1])\
    )
        
    print("sum_count = ", sum_count)
    print("sum_count.count() = ", sum_count.count())
    print("sum_count.collect() = ", sum_count.collect())
   
    # find average per key
    # sum_count = [(k1, (sum1, count1)), (k2, (sum2, count2)), ...]
    averages = sum_count.mapValues(lambda sum_and_count: float(sum_and_count[0]) / float(sum_and_count[1]))
    print("averages = ", averages)
    print("averages.count() = ", averages.count())
    print("averages.collect() = ", averages.collect())
   
    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()