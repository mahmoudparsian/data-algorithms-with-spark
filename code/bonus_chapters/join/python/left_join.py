import sys
from pyspark.sql import SparkSession

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

# find left join(R, S)

# A record format "<key><,><value>"
# A = spark.sparkContext.textFile("/tmp/A.txt");
A = spark.sparkContext.textFile(sys.argv[1]);

# B record format "<key><,><value>"
# B = spark.sparkContext.textFile("/tmp/B.txt");
B = spark.sparkContext.textFile(sys.argv[2]);

# rdd_A: RDD[(String, String)]
rdd_A = A.map(lambda s: s.split(",")).flatMap(lambda s: [(s[0], s[1])])
print("rdd_A=", rdd_A.collect())

# rdd_B: RDD[(String, String)]
rdd_B = B.map(lambda s: s.split(",")).flatMap(lambda s: [(s[0], s[1])])
print("rdd_B=", rdd_B.collect())

"""
RDD.leftOuterJoin(other, numPartitions=None)
Perform a left outer join of self and other.
For each element (k, v) in self, the resulting 
RDD will either contain all pairs (k, (v, w)) 
for w in other, or the pair (k, (v, None)) if 
no elements in other have key k.
Hash-partitions the resulting RDD into the given 
number of partitions.
"""

# A_joined_B: RDD[(String, (String, String))]
A_left_joined_B = rdd_A.leftOuterJoin(rdd_B)
print("A_left_joined_B=", A_left_joined_B.collect())
