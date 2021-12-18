from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Apply a reduceByKey() transformation to an RDD
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
    return (name, number)
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # reduceByKey() transformation
    #
    # source_rdd.reduceByKey(func) --> target_rdd
    #
    # RDD<K,V> reduceByKey(Function2<V,V,V> func)
    # Merge the values for each key using an associative 
    # and commutative reduce function. This will also 
    # perform the merging locally on each mapper before 
    # sending results to a reducer, similarly to a "combiner" 
    # in MapReduce. Output will be hash-partitioned with the 
    # existing partitioner/ parallelism level.
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
    # ADD Numbers per key:
    #
    # apply a reduceByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the city name 
    #       value is sum of number(s)
    #------------------------------------
    rdd3 = rdd2.reduceByKey(lambda a, b: a+b)
    print("rdd3 = ", rdd3)
    print("rdd3.count() = ", rdd3.count())
    print("rdd3.collect() = ", rdd3.collect())


    #------------------------------------
    # Find maximum per key:
    #
    # apply a reduceByKey() transformation to rdd2
    # create a (key, value) pair
    #  where 
    #       key is the city name 
    #       value is sum of number(s)
    #------------------------------------
    rdd4 = rdd2.reduceByKey(lambda a, b: max(a, b))
    print("rdd4 = ", rdd4)
    print("rdd4.count() = ", rdd4.count())
    print("rdd4.collect() = ", rdd4.collect())  
     
    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
