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
T1 = spark.sparkContext.parallelize(d1)
T2 = spark.sparkContext.parallelize(d2)

"""
Return an RDD containing all pairs of elements with 
matching keys in self and other.

Each pair of elements will be returned as a (k, (v1, v2)) 
tuple, where (k, v1) is in self and (k, v2) is in other.

"""

# perform inner join of T1 and T2
joined = T1.join(T2)
print("joined=", joined.collect())

"""
sample run and output:

$SPARK_HOME/bin/spark-submit inner_join_spark.py
joined= 
[
 ('b', (100, 300)), 
 ('b', (100, 400)), 
 ('b', (200, 300)), 
 ('b', (200, 400)), 
 ('a', (10, 40)), 
 ('a', (10, 50)), 
 ('a', (11, 40)), 
 ('a', (11, 50)), 
 ('a', (12, 40)), 
 ('a', (12, 50))
]


"""
