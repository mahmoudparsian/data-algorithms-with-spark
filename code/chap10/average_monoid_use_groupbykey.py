from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#<1> Import the print() function
#<2> Import System-specific parameters and functions
#<3> Import SparkSession from the pyspark.sql module
#<4> Make sure that we have 2 parameters in the command line
#<5> Create an instance of a SparkSession object by using the builder pattern SparkSession.builder class
#<6> Define input path (this can be a file or a directory containing any number of files
#<7> Read input and create the first RDD as RDD[String] where each object has this foramt: "key,number"
#<8> Create key_number_one RDD as (key, number)
#<9> groupByKey() and create [(key1, [v11, v12, ...]), (key2, [v21, v22, ...]), ...]
#<10> Apply the mapValues() transformation to find final average per key


#==========================================
# NOTE:
#
# In general, avoid using groupByKey(), and 
# instead use reduceByKey() or combineByKey().
# For details see: 
#   https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
#
# The groupByKey() solution is provided for educational 
# purposes.  If you need all of the values of a key for 
# some aggregation such as finding the "median" (which you
# need all of the values per key), then  the groupByKey() 
# may be used.
#
#==========================================
# function:  create_pair() to accept  
# a String object as "key,number" and  
# returns a (key, number) pair.
#
# record as String of "key,number"
def create_pair(record):
    tokens = record.split(",")
    # key -> tokens[0] as String
    # number -> tokens[1] as Integer
    return (tokens[0], int(tokens[1]))
# end-of-function
#==========================================
# function:  `add_pairs` accept two
# tuples of (sum1, count1) and (sum2, count2) 
# and returns sum of tuples (sum1+sum2, count1+count2).
#
# a = (sum1, count1)
# b = (sum2, count2)
def add_pairs(a, b):
    # sum = sum1+sum2
    sum = a[0] + b[0]
    # count = count1+count2 
    count = a[1] + b[1]
    return (sum, count)
# end-of-function
#==========================================
def main():
    # <4>    
    if len(sys.argv) != 2:  
        print("Usage: ", __file__, " <input-path>", file=sys.stderr)
        exit(-1)
    #end-if
    
    # <5>
    spark = SparkSession.builder.getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # <6>
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # read input and create an RDD<String>
    # <7>
    records = spark.sparkContext.textFile(input_path) 
    print("records : ", records)
    print("records.count(): ", records.count())
    print("records.collect(): ", records.collect())

    # create a pair of (key, (number, 1)) for "key,number"
    # <8>
    pairs = records.map(create_pair)
    print("pairs : ", pairs)
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())

    # aggregate the (sum, count) of each unique key
    # <9>
    grouped_by_key = pairs.groupByKey() 
    print("grouped_by_key : ", grouped_by_key)
    print("grouped_by_key.count(): ", grouped_by_key.count())
    print("grouped_by_key.collect(): ", grouped_by_key.collect())
    print("grouped_by_key.mapValues(lambda values : list(values)).collect(): ", grouped_by_key.mapValues(lambda values : list(values)).collect())

    # create the final RDD as RDD[key, average]
    # <10>
    # values = (number1, number2, number3, ...) 
    averages =  grouped_by_key.mapValues(lambda values : float(sum(values)) / float(len(values))) 
    print("averages.count(): ", averages.count())
    print("averages.collect(): ", averages.collect())

    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
