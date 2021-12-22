#!/usr/bin/python
#-----------------------------------------------------
# Apply a cartesian() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # cartesian() transformation
    #
    # cartesian(other)
    # Return the Cartesian product of this RDD 
    # and another one, that is, the RDD of all 
    # pairs of elements (a, b) where a is in 
    # self (as source-one RDD) and b is in other
    # (as source-two RDD).
    #
    #========================================
    a = [('a', 2), ('b', 3), ('c', 4)]
    print("a = ", a)
    #
    b = [('p', 50), ('x', 60), ('y', 70), ('z', 80) ]
    print("b = ", b)
    #
    rdd = spark.sparkContext.parallelize(a)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    #
    rdd2 = spark.sparkContext.parallelize(b)
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())
    
    #
    cart = rdd.cartesian(rdd2)
    print("cart = ", cart)
    print("cart.count() = ", cart.count())
    print("cart.collect() = ", cart.collect())
    
    
    # done!
    spark.stop()
#end-def
#==================================
if __name__ == '__main__':
    main()
