"""
Inner-Join in Spark

@author: Mahmoud Parsian

"""

from __future__ import print_function
from pyspark.sql import SparkSession

#---------------------------------------------------------


# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

d1 = [('a', 10), ('a', 11), ('a', 12), ('b', 100), ('b', 200), ('c', 80)]
d2 = [('a', 40), ('a', 50), ('b', 300), ('b', 400), ('d', 90)]
T1 = spark.createDataFrame(d1, ['key', 'value'])
T2 = spark.createDataFrame(d2, ['key2', 'value2'])


# DataFrame.join(other, on=None, how=None)
# perform inner join of T1 and T2
joined = T1.join(T2, T1.key == T2.key2)
joined.show(truncate=False)

"""
sample run and output:

$SPARK_HOME/bin/spark-submit inner_join_dataframe_spark.py

+---+-----+----+------+
|key|value|key2|value2|
+---+-----+----+------+
|a  |10   |a   |40    |
|a  |10   |a   |50    |
|a  |11   |a   |40    |
|a  |11   |a   |50    |
|a  |12   |a   |40    |
|a  |12   |a   |50    |
|b  |100  |b   |300   |
|b  |100  |b   |400   |
|b  |200  |b   |300   |
|b  |200  |b   |400   |
+---+-----+----+------+

"""
