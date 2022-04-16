from __future__ import print_function
import sys
from pyspark.sql import SparkSession

#---------------------------------------
# @author Mahmoud Parsian
#---------------------------------------
# Demo mapPartitions() transformation
# to find (count, minimum, maximum) for
# all given numbers
#---------------------------------------
#
# find (count, min, max) per partition
# partition : single partition with millions of numbers
def custom_function(partition):
  local_count = 0
  local_min = sys.maxsize
  local_max = -sys.maxsize
  # iterate partition one number in a time
  for rec in partition:
    int_num = int(rec.strip())
    local_count += 1
    local_min = min(int_num, local_min)
    local_max = max(int_num, local_max)
  #end-for
  return [(local_count, local_min, local_max)]
#end-def    
#---------------------------------------
#
# x = (count_1, min_1, max_1)
# y = (count_2, min_2, max_2)
def add_triplets(x, y):
  local_count = x[0] + y[0]
  local_min = min(x[1], y[1])
  local_max = max(x[2], y[2])
  return (local_count, local_min, local_max)
#end-def
#---------------------------------------
def debug_partition(partition):
  print("partition elements: ", list(partition))
#end-def
#---------------------------------------

# step-1: define input path
input_path = sys.argv[1]
print("input_path=", input_path)

# step-2: create an instance of SparkSession 
spark = SparkSession.builder.getOrCreate()
print("spark.version=", spark.version)

# step-3: read input and create RDD[String]
rdd = spark.sparkContext.textFile(input_path)
print("rdd.collect()=", rdd.collect())
print("rdd.getNumPartitions()=", rdd.getNumPartitions())
# debug: what are in each partition
rdd.foreachPartition(debug_partition)

# step-4: apply mapPartitions() and 
# create (count, min, max) per partition
triplets = rdd.mapPartitions(custom_function)
print("triplets.count()=", triplets.count())
print("triplets.collect()=", triplets.collect())

# step-5: find the final (count, min, max) for all partitions
final_result = triplets.reduce(add_triplets)
print("final_result", final_result)

# step-6: done
spark.stop()
