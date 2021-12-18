from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Apply a flatMap() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------



#=========================================
def tokenize(record):
    tokens = record.split()
    mylist = []
    for word in tokens:
        if len(word) > 2:
            mylist.append(word)
        #end-if
    #end-for
    return mylist
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # flatMap() transformation
    #
    # source_rdd.flatMap(function) --> target_rdd
    #
    # flatMap() is a 1-to-M transformation
    #
    # flatMap(f, preservesPartitioning=False)[source]
    # Return a new RDD by first applying a function 
    # to all elements of this RDD, and then flattening 
    # the results.
    #
    #========================================
    # Create a list of tuples. 
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    list_of_strings= ['of', 'a fox jumped', 'fox jumped of fence', 'a foxy fox jumped high']
    print("list_of_strings = ", list_of_strings)
    rdd = spark.sparkContext.parallelize(list_of_strings)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    
    #------------------------------------
    # 1. apply a flatMap() transformation to rdd
    # 2. tokenize each string and then flatten words
    # 3. Ignore words if length is less than 3
    #------------------------------------
    rdd2 = rdd.flatMap(lambda rec : tokenize(rec))
    print("rdd2 = ", rdd2)
    print("rdd2.count() = ", rdd2.count())
    print("rdd2.collect() = ", rdd2.collect())

    
    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
    