from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#-----------------------------------------------------
# Apply a aggregateByKey() transformation to an
# RDD[(key, value)] to find average per key.
# Input: NONE
# Use collections to crete RDD[(key, value)]
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=========================================
# Create a pair of (name, number) from t3
# t3 = (name, city, number)
def create_pair(t3):
    #
    name = t3[0]
    #
    #city = t3[1]
    #
    number = int(t3[2])
    # create a pair
    return (name, number)
#end-def
#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    #

    #========================================
    # aggregateByKey() transformation
    #
    # source_rdd.aggregateByKey(zeroValue, lambda1, lambda2) --> target_rdd
    #
    # aggregateByKey(
    #   zeroValue,
    #   seqFunc,
    #   combFunc,
    #   numPartitions=None,
    #   partitionFunc=<function portable_hash>
    # )
    #
    # Aggregate the values of each key, using given combine
    # functions and a neutral "zero value". This function can
    # return a different result type, U, than the type of the
    # values in this RDD, V. Thus, we need one operation for
    # merging a V into a U and one operation for merging two U's,
    # The former operation is used for merging values within a
    # partition, and the latter is used for merging values between
    # partitions. To avoid memory allocation, both of these functions
    # are allowed to modify and return their first argument instead
    # of creating a new U.
    #
    # We will use (SUM, COUNT) as a combined data structure
    # for finding the sum of the values and count of the
    # values per key. By KEY, simultaneously calculate the SUM
    # (the numerator for the average that we want to compute),
    # and COUNT (the denominator for the average that we want
    # to compute):
    #
    # zero_value = (0,0)
    #
    # rdd2 = rdd1.aggregateByKey(
    #    zero_value,
    #    lambda a,b: (a[0] + b,    a[1] + 1),
    #    lambda a,b: (a[0] + b[0], a[1] + b[1])
    # )
    #
    # Where the following is true about the meaning of each a and b
    # pair above (so you can visualize what's happening):
    #
    # First lambda expression for Within-Partition Reduction Step::
    #    a: is a TUPLE that holds: (runningSum, runningCount).
    #    b: is a SCALAR that holds the next Value
    #
    # Second lambda expression for Cross-Partition Reduction Step::
    #    a: is a TUPLE that holds: (runningSum, runningCount).
    #    b: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).
    #
    # Finally, calculate the average for each KEY, and collect results.
    #
    #  finalResult = rdd2.mapValues(lambda v: v[0]/v[1]).collect()
    #========================================
    #
    # Create a list of tuples.
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    list_of_tuples= [('alex','Sunnyvale', 25),\
                     ('alex','Sunnyvale', 33),\
                     ('alex','Sunnyvale', 45),\
                     ('alex','Sunnyvale', 63),\
                     ('mary', 'Ames', 22),\
                     ('mary', 'Cupertino', 66),\
                     ('mary', 'Ames', 20),\
                     ('bob', 'Ames', 26)]
    #
    print("list_of_tuples = ", list_of_tuples)
    #
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
    # apply a aggregateByKey() transformation to rdd2
    # create a (key, value) pair
    #  where
    #       key is the name
    #       value is the (sum, count)
    #------------------------------------
    # v : a number for a given (key, v) pair of source RDD
    # C : is a tuple of pair (sum, count), which is
    # also called a "combined" data structure
    #
    # (0, 0) denotes a zero value per partition
    # where (0, 0 ) = (sum, count)
    # Each partition starts with (0, 0) as an initial value
    #
    sum_count = rdd2.aggregateByKey(\
        (0, 0),\
        lambda C, v: (C[0]+v, C[1]+1),\
        lambda C1, C2: (C1[0]+C2[0], C1[1]+C2[1])\
    )

    print("sum_count = ", sum_count)
    print("sum_count.count() = ", sum_count.count())
    print("sum_count.collect() = ", sum_count.collect())

    #===============================
    # find average per key
    # sum_count = [(k1, (sum1, count1)), (k2, (sum2, count2)), ...]
    #===============================
    averages = sum_count.mapValues(lambda sum_and_count: float(sum_and_count[0]) / float(sum_and_count[1]))
    print("averages = ", averages)
    print("averages.count() = ", averages.count())
    print("averages.collect() = ", averages.collect())

    # done!
    spark.stop()
#==========================================

if __name__ == '__main__':
    main()
    
