from __future__ import print_function
from pyspark.sql import SparkSession

#-----------------------------------------------------
# flatMap() is a 1-to-Many transformation:
# each source element can be mapped to zero, 
# 1, 2, 3, ... , or more elements
#
# Apply a flatMap() transformation to an RDD
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


    #========================================
    # flatMap() transformation
    #
    # source_rdd.flatMap(function) --> target_rdd
    #
    # flatMap() is a 1-to-Many transformation
    #
    # PySpark's flatMap() is a transformation operation that flattens 
    # the RDD/DataFrame (array/map DataFrame columns) after applying the 
    # function on every element and returns a new PySpark RDD/DataFrame.
    #
    #========================================
    # Create a list of arrays.
    list_of_arrays= [ \
                      ['item_11','item_12', 'item_13'], \
                      ['item_21','item_22'], \
                      [], \
                      ['item_31','item_32', 'item_33', 'item_34'], \
                      [] \
                    ]
    print("list_of_arrays = ", list_of_arrays)
    
    # create rdd : RDD[(name, city, age)]
    rdd = spark.sparkContext.parallelize(list_of_arrays)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    #------------------------------------
    # apply a map() transformation to rdd
    #------------------------------------
    # rdd2 : RDD[[String]]
    rdd_mapped = rdd.map(lambda t : t)
    print("rdd_mapped = ", rdd_mapped)
    print("rdd_mapped.count() = ", rdd_mapped.count())
    print("rdd_mapped.collect() = ", rdd_mapped.collect())


    #------------------------------------
    # apply a flatMap() transformation to rdd
    # Note-1: empty lists will be dropped
    # Note-2: [str1, str2, str3] will be flattened 
    # and mapped to 3 target elements as str1, str2, and str3
    #------------------------------------
    # rdd_flatmapped : RDD[String], 
    rdd_flatmapped = rdd.flatMap(lambda t : t)
    print("rdd_flatmapped = ", rdd_flatmapped)
    print("rdd_flatmapped.count() = ", rdd_flatmapped.count())
    print("rdd_flatmapped.collect() = ", rdd_flatmapped.collect())

    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()

"""
sample run:

export Spark_HOME=/book/spark-3.2.0     
$SPARK_HOME/bin/spark-submit flatmap_transformation_1_from_collection.py                                   (master)data-algorithms-with-spark
list_of_arrays =  
[
 ['item_11', 'item_12', 'item_13'], 
 ['item_21', 'item_22'], 
 [], 
 ['item_31', 'item_32', 'item_33', 'item_34'], 
 []
]
rdd =  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
rdd.count() =  5
rdd.collect() =  
[
 ['item_11', 'item_12', 'item_13'], 
 ['item_21', 'item_22'], 
 [], 
 ['item_31', 'item_32', 'item_33', 'item_34'], 
 []
]
rdd_mapped =  PythonRDD[2] at RDD at PythonRDD.scala:53
rdd_mapped.count() =  5
rdd_mapped.collect() =  
[
 ['item_11', 'item_12', 'item_13'], 
 ['item_21', 'item_22'], 
 [], 
 ['item_31', 'item_32', 'item_33', 'item_34'], 
 []
]
rdd_flatmapped =  PythonRDD[4] at RDD at PythonRDD.scala:53
rdd_flatmapped.count() =  9
rdd_flatmapped.collect() =  
[
 'item_11', 
 'item_12', 
 'item_13', 
 'item_21', 
 'item_22', 
 'item_31', 
 'item_32', 
 'item_33', 
 'item_34'
]

"""