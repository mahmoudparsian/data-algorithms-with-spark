#!/usr/bin/env python
#-----------------------------------------------------
#
# NOTE:
#      This solution creates a SINGLE FILE 
#      per created partition 
#
#-----------------------------------------------------
# 1. Read customer.txt 
# 2. Create a DataFrame with 4 columns: 
#    { <customer_id>, 
#      <year>, 
#      <transaction_id>, 
#      <transaction_value> }
# 3. Partition data by (<customer_id>, <year>)
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession

# define input path
input_path = sys.argv[1]
print("input_path=", input_path)

# define output path for partitioned data
output_path = sys.argv[2]
print("output_path=", output_path)

# create a SparkSession object
spark = SparkSession.builder.getOrCreate()
       

# create a DataFrame, note that toDF() returns a 
# new DataFrame with new specified column names
# columns = ('customer_id', 'year', 'transaction_id', 'transaction_value')
df = spark.read.option("inferSchema", "true")\
  .csv(input_path)\
  .toDF('customer_id', 'year', 'transaction_id', 'transaction_value')
#
df.show(truncate=False)
df.printSchema()
#
# partition data by 'customer_id', and then by 'year' 
# and create a SINFGLE FILE per created partition.
# DataFrame.repartition('customer_id', 'year') qurantees
# a single file per partition.
df.repartition('customer_id', 'year')\
  .write.partitionBy('customer_id', 'year')\
  .parquet(output_path)

# read the partitioned data back to another DataFrame
df2 = spark.read.parquet(output_path)
df2.show(truncate=False)
df2.printSchema()

# done!
spark.stop()
