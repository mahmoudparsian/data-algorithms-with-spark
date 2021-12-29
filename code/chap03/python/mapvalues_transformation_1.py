from __future__ import print_function
from pyspark.sql import SparkSession

#-----------------------------------------------------
# RDD.mapValues(f)
# Pass each value in the key-value pair RDD through 
# a map function without changing the keys; this also 
# retains the original RDD’s partitioning.
#
# Note that mapValues() can be accomplished by map().
#
# Let rdd = RDD[(K, V)]
# then the following RDDs (rdd2, rdd3) are equivalent:
#
#    rdd2 = rdd.mapValues(f)
#
#    rdd3 = rdd.map(lambda x: (x[0], f(x[1])))
#
# print() is used for educational purposes.
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a list of arrays.
    list_of_key_value= [ ("k1", (120, 12)), ("k1", (100, 5)), ("k2", (12, 2)), ("k1", (10, 5))]
    print("list_of_key_value = ", list_of_key_value)
    
    # create rdd : RDD[(String, (Integer, Integer))]
    # key: String
    # value = (Integer, Integer)
    rdd = spark.sparkContext.parallelize(list_of_key_value)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    #------------------------------------
    # apply a mapValues() transformation to rdd
    #------------------------------------
    # rdd_mapped : RDD[(String, Integer)]
    #
    rdd_mapped = rdd.mapValues(lambda v : v[0] / v[1])
    print("rdd_mapped = ", rdd_mapped)
    print("rdd_mapped.count() = ", rdd_mapped.count())
    print("rdd_mapped.collect() = ", rdd_mapped.collect())


    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()

"""
sample run:

export Spark_HOME=/book/spark-3.2.0     
$SPARK_HOME/bin/spark-submit mapvalues_transformation_1.py    
list_of_key_value =  [('k1', (120, 12)), ('k1', (100, 5)), ('k2', (12, 2)), ('k1', (10, 5))]

rdd =  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
rdd.count() =  4
rdd.collect() =  [('k1', (120, 12)), ('k1', (100, 5)), ('k2', (12, 2)), ('k1', (10, 5))]

rdd_mapped =  PythonRDD[2] at RDD at PythonRDD.scala:53
rdd_mapped.count() =  4
rdd_mapped.collect() =  [('k1', 10.0), ('k1', 20.0), ('k2', 6.0), ('k1', 2.0)]

"""