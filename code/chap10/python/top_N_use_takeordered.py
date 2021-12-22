from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#-----------------------------------------------------
# Find Top-N per key
# Input: N
#------------------------------------------------------
# Input Parameters:
#    N
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <N>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("top_N_use_takeordered")\
        .getOrCreate()
    #
    print("spark=",  spark)

    # Top-N
    N = int(sys.argv[1])
    print("N : ", N)

    #=====================================
    # Create an RDD from a list of values
    #=====================================
    list_of_key_value = [
        ("a", 1), ("a", 7), ("a", 2), ("a", 3), 
        ("b", 2), ("b", 4),
        ("c", 10), ("c", 50), ("c", 60), ("c", 70),
        ("d", 5), ("d", 15), ("d", 25),
        ("e", 1), ("e", 2),
        ("f", 9), ("f", 2),
        ("g", 22)
    ]
    #   
    print("list_of_key_value = ",  list_of_key_value)
    #
    
    #=====================================
    # create an RDD from a collection
    # Distribute a local Python collection to form an RDD
    #=====================================
    rdd = spark.sparkContext.parallelize(list_of_key_value)
    print("rdd=",  rdd)
    print("rdd.count=",  rdd.count())
    print("rdd.collect()=",  rdd.collect())
    
    #=====================================
    # Make sure same keys are combined
    #=====================================
    combined = rdd.reduceByKey(lambda a, b: a+b)
    print("combined=",  combined)
    print("combined.count=",  combined.count())
    print("combined.collect()=",  combined.collect())
    #
    
    #=====================================
    # find Top-N
    #===================================== 
    # x = (key, number)
    # x[0] --> key
    # x[1] --> number   
    topN = combined.takeOrdered(N, key=lambda x: -x[1])
    print("topN = ",  topN)
    
    #=====================================
    # find Bottom-N
    #===================================== 
    # x = (key, number)
    # x[0] --> key
    # x[1] --> number   
    bottomN = combined.takeOrdered(N, key=lambda x: x[1])
    print("bottomN = ",  bottomN)


    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()