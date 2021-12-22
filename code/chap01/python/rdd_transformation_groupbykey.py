from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Apply a groupByKey() transformation to an RDD
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
    city = t3[1]
    number = t3[2]
    return (name, (city, number))
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # groupByKey() transformation
    #
    # source_rdd.groupByKey() --> target_rdd
    #
    # Group the values for each key in the RDD 
    # into a single sequence. Hash-partitions the 
    # resulting RDD with the existing partitioner/
    # parallelism level.
    #
    # Note: If you are grouping in order to perform 
    # an aggregation (such as a sum or average) over 
    # each key, using reduceByKey() or combineByKey()
    # will provide much better performance.
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
    #       value is a of tuple of (city, number)
    #------------------------------------
    rdd2 = rdd.map(lambda t : create_pair(t))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())


    #------------------------------------
    # apply a groupByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the city name 
    #       value is the Iterable<(city, number)>
    #------------------------------------
    rdd3 = rdd2.groupByKey()
    print("rdd3 = ", rdd3)
    print("rdd3.count() = ", rdd3.count())
    print("rdd3.collect() = ", rdd3.collect())
    print("rdd3.mapValues().collect() = ", rdd3.mapValues(lambda values: list(values)).collect())
   
    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
