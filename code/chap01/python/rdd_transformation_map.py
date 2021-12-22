from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Apply a map() transformation to an RDD
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
    key = t3[0]
    value = t3[2]
    return (key, value)
#end-def
#==========================================
def create_pair_city(t3):
    # t3 = (name, city, number)
    key = t3[1]
    value = t3
    return (key, value)

#==========================================
def main():
   
    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # map() transformation
    #
    # source_rdd.map(function) --> target_rdd
    #
    # map() is a 1-to-1 transformation
    #
    # map(f, preservesPartitioning=False)[source]
    # Return a new RDD by applying a function to each 
    # element of this RDD.
    #
    #========================================
    # Create a list of tuples. 
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    list_of_tuples= [('alex','Sunnyvale', 25), \
                     ('alex','Sunnyvale', 33), 
                     ('mary', 'Ames', 22), \
                     ('mary', 'Cupertino', 66), \
                     ('jane', 'Ames', 20), \
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
    #       value is the last element of tuple
    #------------------------------------
    rdd2 = rdd.map(lambda t : create_pair(t))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())


    #------------------------------------
    # apply a map() transformation to rdd
    # create a (key, value) pair
    #  where 
    #       key is the city name (second element of tuple)
    #       value is the entire tuple
    #------------------------------------
    rdd3 = rdd.map(lambda t : create_pair_city(t))
    print("rdd3 = ", rdd3)
    print("rdd3.count() = ", rdd3.count())
    print("rdd3.collect() = ", rdd3.collect())

    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
