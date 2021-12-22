from __future__ import print_function
from pyspark.sql import SparkSession
import sys

#-----------------------------------------------------
# map() is a 1-to-1 transformation
# Apply a map() transformation to an RDD
# Input: NONE
#
# print() is used for educational purposes.
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------


#=========================================
# t = "name,city,age"
# create a (key, value) pair 
def create_pair(t):
    tokens = t.split(",")
    # tokens = (name, city, age)
    name = tokens[0]
    age = int(tokens[2])
    return (name, age)
#end-def
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
    # Return a new RDD by applying a function to 
    # each element of this source RDD.
    #
    #========================================
    # Create a list of tuples.
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    
    # define input path
    input_path = sys.argv[1]
    print("input_path = ", input_path)

    # create rdd : RDD[String]
    rdd = spark.sparkContext.textFile(input_path)
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
    # rdd2 : RDD[(name, age)]
    rdd2 = rdd.map(lambda t : create_pair(t))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())


    #------------------------------------
    # increment age by 5
    #------------------------------------
    # rdd3 : RDD[(name, age)], where age is incremented by 5
    rdd3 = rdd2.mapValues(lambda v : v+5)
    print("rdd3 = ", rdd3)
    print("rdd3.count() = ", rdd3.count())
    print("rdd3.collect() = ", rdd3.collect())

    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
    
"""
sample run:

$Spark_HOME/bin/spark-submit map_transformation_1_from_file.py sample_input               
input_path =  sample_input

rdd =  sample_input MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
rdd.count() =  8
rdd.collect() =  ['alex,Sunnyvale,25', 'alex,Sunnyvale,33', 'mary,Ames,22', 'mary,Cupertino,66', 'mary,Sunnyvale,44', 'jane,Ames,20', 'jane,Troy,40', 'bob,Ames,26']

rdd2 =  PythonRDD[3] at RDD at PythonRDD.scala:53
rdd2.count() =  8
rdd2.collect() =  [('alex', 25), ('alex', 33), ('mary', 22), ('mary', 66), ('mary', 44), ('jane', 20), ('jane', 40), ('bob', 26)]

rdd3 =  PythonRDD[5] at RDD at PythonRDD.scala:53
rdd3.count() =  8
rdd3.collect() =  [('alex', 30), ('alex', 38), ('mary', 27), ('mary', 71), ('mary', 49), ('jane', 25), ('jane', 45), ('bob', 31)]


"""