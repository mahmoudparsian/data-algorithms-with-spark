import sys
from pyspark.sql import SparkSession

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

# find right join(A, B)

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
pyspark.RDD.rightOuterJoin
RDD.rightOuterJoin(other, numPartitions=None)
Perform a right outer join of self and other.
For each element (k, w) in other, the resulting 
RDD will either contain all pairs (k, (v, w)) 
for v in this, or the pair (k, (None, w)) if no 
elements in self have key k.
Hash-partitions the resulting RDD into the given 
number of partitions.

"""
# A_joined_B: RDD[(String, (String, String))]
A_right_joined_B = rdd_A.rightOuterJoin(rdd_B)
print("A_right_joined_B=", A_right_joined_B.collect())
