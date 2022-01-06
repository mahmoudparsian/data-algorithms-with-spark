#!/usr/bin/python
#-----------------------------------------------------
# This program find approx. median per key using
# the percentile_approx function.
#
"""
pyspark.sql.functions.percentile_approx(col, percentage, accuracy=10000)
Returns the approximate percentile of the numeric column col which is the 
smallest value in the ordered col values (sorted from least to greatest) 
such that no more than percentage of col values is less than the value or 
equal to that value. The value of percentage must be between 0.0 and 1.0.

The accuracy parameter (default: 10000) is a positive numeric literal which 
controls approximation accuracy at the cost of memory. Higher value of accuracy 
yields better accuracy, 1.0/accuracy is the relative error of the approximation.

When percentage is an array, each value of the percentage array must be between 
0.0 and 1.0. In this case, returns the approximate percentile array of column 
col at the given percentage array.
"""

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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import randn
from pyspark.sql.functions import percentile_approx
import statistics

#------------------------------------------------------------
def create_test_dataframe(spark_session, number_of_keys, number_of_rows):
    key = (col("id") % number_of_keys).alias("key")
    value = (randn(41) + key * number_of_keys).alias("value")
    df = spark_session.range(0, number_of_rows, 1, 1).select(key, value)
    return df
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
    
    # relative error = 1/10,000,000
    # use approximation
    approx_median_per_key = df.groupBy("key").agg(percentile_approx("value", 0.5, lit(100000000)).alias("median"))
    print("approx_median_per_key.count()=", approx_median_per_key.count())
    approx_median_per_key.printSchema()
    approx_median_per_key.show(truncate=False)
    
    # for comparison purposes 
    # use exact meidan
    exact_median_per_key = df.rdd.groupByKey().mapValues(statistics.median).toDF(["key", "value"])
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

$SPARK_HOME/bin/spark-submit dataframe_median_approx.py

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

median_per_key.count()= 10
root
 |-- key: long (nullable = true)
 |-- median: double (nullable = true)

+---+---------------------+
|key|median               |
+---+---------------------+
|0  |0.0033050503181008034|
|1  |10.00094628670667    |
|2  |20.005500379821054   |
|3  |29.99743993035764    |
|4  |40.007756160144034   |
|5  |50.00142206966906    |
|6  |60.00119023425729    |
|7  |69.99867587504745    |
|8  |79.99624546363756    |
|9  |89.99589586573916    |
+---+---------------------+

exact_median_per_key.count()= 10
root
 |-- key: long (nullable = true)
 |-- value: double (nullable = true)

+---+---------------------+
|key|value                |
+---+---------------------+
|0  |0.0033093587268769875|
|1  |10.000949376174427   |
|2  |20.005507370147917   |
|3  |29.997440116995826   |
|4  |40.00776207511166    |
|5  |50.001441129991015   |
|6  |60.00119836061273    |
|7  |69.9986917086938     |
|8  |79.99626717509605    |
|9  |89.99589801642136    |
+---+---------------------+


"""