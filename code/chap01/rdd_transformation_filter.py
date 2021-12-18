from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a filter() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=========================================
def age_filter_26(t3):
    # t3 = (name, city, age)
    # name = t3[0]
    # city = t3[1]
    age = t3[2]
    if (age > 26):
        # keep the element
        return True
    else:
        # filter it out
        return False
#end-def
#=========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    #========================================
    # filter() transformation
    #
    #  filter(f)
    #  Description:
    #      Return a new RDD containing only the 
    #      elements that satisfy a predicate.
    #
    #========================================
    # Create a list of tuples. 
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    list_of_tuples= [('alex','Sunnyvale', 25), \
                     ('alex','Sunnyvale', 33), \
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
    # apply a filter() transformation to rdd
    # and keep records if the age is > 26.
    # Use Lambda Expression
    #------------------------------------
    # t3[2] is the 3rd item of Tuple3
    filtered_by_lambda = rdd.filter(lambda tuple3 : tuple3[2] > 26)
    print("filtered_by_lambda = ", filtered_by_lambda)
    print("filtered_by_lambda.count() = ", filtered_by_lambda.count())
    print("filtered_by_lambda.collect() = ", filtered_by_lambda.collect())


    #------------------------------------
    # apply a filter() transformation to rdd
    # and keep records if the age is > 26.
    # Use Python function
    #------------------------------------
    # t3[2] is the 3rd item of Tuple3
    filtered_by_function = rdd.filter(age_filter_26)
    print("filtered_by_function = ", filtered_by_function)
    print("filtered_by_function.count() = ", filtered_by_function.count())
    print("filtered_by_function.collect() = ", filtered_by_function.collect())
#end-def
#=========================================

if __name__ == '__main__':
    main()
    
