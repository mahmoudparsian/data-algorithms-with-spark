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

    # Create a list of (K, V)
    list_of_key_value= [ ("k1", [1, 2, 3]), ("k1", [4, 5, 6, 7]), ("k2", [6, 7, 8]), ("k1", [1, 3])]
    print("list_of_key_value = ", list_of_key_value)
    
    # create rdd : RDD[(String, [Integer])]
    # key: String
    # value = [Integer]
    rdd = spark.sparkContext.parallelize(list_of_key_value)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    #------------------------------------
    # apply a mapValues() transformation to rdd
    #------------------------------------
    # find (count and average) for given set of values
    # rdd_mapped : RDD[(String, (Integer, Float))]
    # rdd_mapped : RDD[(String, (count, average))]
    #
    rdd_count_avg = rdd.mapValues(lambda v : (len(v), float(sum(v))/len(v)))
    print("rdd_count_avg = ", rdd_count_avg)
    print("rdd_count_avg.count() = ", rdd_count_avg.count())
    print("rdd_count_avg.collect() = ", rdd_count_avg.collect())


    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()

"""
sample run:

export Spark_HOME=/book/spark-3.2.0     
$SPARK_HOME/bin/spark-submit mapvalues_transformation_2.py    

list_of_key_value =  [('k1', [1, 2, 3]), ('k1', [4, 5, 6, 7]), ('k2', [6, 7, 8]), ('k1', [1, 3])]

rdd =  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
rdd.count() =  4
rdd.collect() =  [('k1', [1, 2, 3]), ('k1', [4, 5, 6, 7]), ('k2', [6, 7, 8]), ('k1', [1, 3])]

rdd_count_avg =  PythonRDD[2] at RDD at PythonRDD.scala:53
rdd_count_avg.count() =  4
rdd_count_avg.collect() =  [('k1', (3, 2.0)), ('k1', (4, 5.5)), ('k2', (3, 7.0)), ('k1', (2, 2.0))]

"""