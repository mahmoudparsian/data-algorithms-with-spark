from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a foldByKey() transformation to an 
# RDD[key, value] to find average per key.
# Input: NONE
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
    return (name, (number, 1))
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    #

    #========================================
    # foldByKey() transformation:
    #
    # foldByKey(
    #   zeroValue, 
    #   func, 
    #   numPartitions=None, 
    #   partitionFunc=<function portable_hash>
    # )
    #
    # Merge the values for each key using an associative 
    # function “func” and a neutral "zeroValue" which may 
    # be added to the result an arbitrary number of times, 
    # and must not change the result (e.g., 0 for addition, 
    # or 1 for multiplication.).    
    #
    # source_rdd.foldByKey() --> target_rdd
    #
    #========================================
    #
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
    #       value is a of tuple of (number, 1)
    #------------------------------------
    rdd2 = rdd.map(lambda t : create_pair(t))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())


    #------------------------------------
    # apply a foldByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the name 
    #       value is the (sum, count)
    #------------------------------------
    # x = (sum1, count1)
    # y = (sum2, count2)
    # x[0]+y[0] --> sum1+sum2
    # x[1]+y[1] --> count1+count2
    #
    # zero_value = (0, 0) = (sum, count)
    sum_count = rdd2.foldByKey(\
        (0, 0),\
        lambda x, y: (x[0]+y[0], x[1]+y[1])\
    )
    #
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
#=======================================
if __name__ == '__main__':
    main()