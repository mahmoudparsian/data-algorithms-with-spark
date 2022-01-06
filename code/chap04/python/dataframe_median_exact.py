#!/usr/bin/python
#-----------------------------------------------------
# This program find the exact median per key.
#
#------------------------------------------------------
# Note-1: print(), collect(), and show() are used 
# for debugging and educational purposes only.
#
#------------------------------------------------------
# Input Parameters:
#    none
#-------------------------------------------------------
#
# @author Mahmoud Parsian
#-------------------------------------------------------
#
from __future__ import print_function
import sys
import statistics
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import randn
from pyspark.sql.functions import percentile_approx
from pyspark.sql.functions import collect_list
from pyspark.sql.types import FloatType

#------------------------------------------------------------
def create_test_dataframe(spark_session, number_of_keys, number_of_rows):
    key = (col("id") % number_of_keys).alias("key")
    value = (randn(41) + key * number_of_keys).alias("value")
    df = spark_session.range(0, number_of_rows, 1, 1).select(key, value)
    return df
#end-def
#------------------------------------------------------------
def calculate_median(list_of_numbers):
    return statistics.median(list_of_numbers)
#end-def
#------------------------------------------------------------

def main():
    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    # create a DataFrame with 1000,000 rows and two columns: "key" and "value"
    # number of keys will be 10 {0, 1, 2,, ..., 9}
    df = create_test_dataframe(spark, 10, 1000000)
    print("df.count()=", df.count())
    df.printSchema()
    df.show(20, truncate=False)
    
    # create a UDF from a python function:
    # FloatType() is a return type of function calculate_median(list)
    calculate_median_udf = udf(lambda L: calculate_median(L), FloatType())
    
    # relative error = 1/10,000,000
    # use approximation df2.agg(collect_list('age'))
    exact_median_per_key = df.groupBy("key").agg(calculate_median_udf(collect_list('value')).alias("median"))
    print("exact_median_per_key.count()=", exact_median_per_key.count())
    exact_median_per_key.printSchema()
    exact_median_per_key.show(truncate=False) 
    
       
    # done!
    spark.stop()
#end-def
#------------------------------------------------------------

if __name__ == '__main__':
    main()


"""

sample run:

$SPARK_HOME/bin/spark-submit dataframe_median_exact.py

df.count()= 1000000
root
 |-- key: long (nullable = true)
 |-- value: double (nullable = true)

+---+------------------+
|key|value             |
+---+------------------+
|0  |2.068156022316271 |
|1  |8.027554192961778 |
|2  |19.52018217402543 |
|3  |31.169196967545815|
|4  |41.23563916101306 |
|5  |50.047013240108114|
|6  |59.63962009959363 |
|7  |69.60849350342053 |
|8  |79.0852103414974  |
|9  |91.2801670104037  |
|0  |0.3652175377373015|
|1  |9.583712586334386 |
|2  |20.302347393960105|
|3  |29.912762966182612|
|4  |39.977451351340655|
|5  |49.81893560890755 |
|6  |59.87226558951008 |
|7  |69.95091076459876 |
|8  |80.78822400704684 |
|9  |89.49925747255001 |
+---+------------------+
only showing top 20 rows

exact_median_per_key.count()= 10
root
 |-- key: long (nullable = true)
 |-- median: float (nullable = true)

+---+------------+
|key|median      |
+---+------------+
|0  |0.0033093588|
|1  |10.000949   |
|2  |20.005507   |
|3  |29.99744    |
|4  |40.007763   |
|5  |50.001442   |
|6  |60.001198   |
|7  |69.998695   |
|8  |79.99627    |
|9  |89.995895   |
+---+------------+
"""