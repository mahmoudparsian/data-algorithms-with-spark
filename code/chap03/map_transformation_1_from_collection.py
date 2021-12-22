from __future__ import print_function
from pyspark.sql import SparkSession

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
# t3 = (name, city, age)
# create a (key, value) pair 
def create_pair(t3):
    name = t3[0]
    age = t3[2]
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
    list_of_tuples= [('alex','Sunnyvale', 25), \
                     ('alex','Sunnyvale', 33),
                     ('mary', 'Ames', 22), \
                     ('mary', 'Cupertino', 66), \
                     ('jane', 'Ames', 20), \
                     ('bob', 'Ames', 26)]
    print("list_of_tuples = ", list_of_tuples)
    
    # create rdd : RDD[(name, city, age)]
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

export Spark_HOME=/Users/mparsian/spark-3.2.0     
$Spark_HOME/bin/spark-submit map_transformation_1_from_collection.py        

list_of_tuples =  
[
('alex', 'Sunnyvale', 25), 
('alex', 'Sunnyvale', 33), 
('mary', 'Ames', 22), 
('mary', 'Cupertino', 66), 
('jane', 'Ames', 20), 
('bob', 'Ames', 26)
]
rdd =  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
rdd.count() =  6
rdd.collect() =  
[
('alex', 'Sunnyvale', 25), 
('alex', 'Sunnyvale', 33), 
('mary', 'Ames', 22), 
('mary', 'Cupertino', 66), 
('jane', 'Ames', 20), 
('bob', 'Ames', 26)
]
rdd2 =  PythonRDD[2] at RDD at PythonRDD.scala:53
rdd2.count() =  6
rdd2.collect() =  
[
('alex', 25), 
('alex', 33), 
('mary', 22), 
('mary', 66), 
('jane', 20), 
('bob', 26)
]
rdd3 =  PythonRDD[4] at RDD at PythonRDD.scala:53
rdd3.count() =  6
rdd3.collect() =  
[
('alex', 30), 
('alex', 38), 
('mary', 27), 
('mary', 71), 
('jane', 25), 
('bob', 31)
]

"""