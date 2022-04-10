#!/usr/bin/env python
#-----------------------------------------------------
# 1. Read customer.txt 
# 2. Create a DataFrame with 4 columns: 
#    { 
#      <customer_id>, 
#      <date>, 
#      <transaction_id>, 
#      <amount> 
#    }
#
# <date> as day/month/year
#
# 3. Partition data by (<year>, <month>)
#
# sample input: customers_with_date.txt
#
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

#-------------------------------------
# date_as_str: day/month/year
@udf(returnType=IntegerType())
def get_year(date_as_str):
    tokens = date_as_str.split("/")
    return int(tokens[2])
#end-def
#-------------------------------------
# date_as_str: day/month/year
@udf(returnType=IntegerType())
def get_month(date_as_str):
    tokens = date_as_str.split("/")
    return int(tokens[1])
#end-def
#-------------------------------------
# main program:
#
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
# columns = ('customer_id', 'date', 'transaction_id', 'amount')
df = spark.read.csv(input_path)\
  .toDF('customer_id', 'date', 'transaction_id', 'amount')
#
print("df::")
df.show(truncate=False)
df.printSchema()

#
# add 2 new columns: year and month 
df2 = df.withColumn('year', get_year(df.date))\
        .withColumn('month', get_month(df.date))
#
print("df2::")
df2.show(truncate=False)
df2.printSchema()
#
# partition data by 'year', and then by 'month'
# each partition will have one or more files
df2.write.partitionBy('year', 'month')\
   .parquet(output_path)

# read the partitioned data back to another DataFrame
df3 = spark.read.parquet(output_path)
print("df3::")
df3.show(truncate=False)
df3.printSchema()

# done!
spark.stop()
