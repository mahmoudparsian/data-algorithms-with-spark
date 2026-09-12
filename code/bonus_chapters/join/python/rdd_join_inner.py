import sys
from pyspark.sql import SparkSession

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

# find inner join(A, B)

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
pyspark.RDD.join
RDD.join(other, numPartitions=None)
Return an RDD containing all pairs of elements 
with matching keys in self and other.
Each pair of elements will be returned as a 
(k, (v1, v2)) tuple, where (k, v1) is in self 
and (k, v2) is in other.
Performs a hash join across the cluster.
"""
# A_joined_B: RDD[(String, (String, String))]
A_joined_B = rdd_A.join(rdd_B)
print("A_joined_B=", A_joined_B.collect())
