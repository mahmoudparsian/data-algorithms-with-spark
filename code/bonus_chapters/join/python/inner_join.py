import sys
from pyspark.sql import SparkSession

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

# find inner join(R, S)

# R record format "<key><,><value>"
# R = spark.sparkContext.textFile("/tmp/R.txt");
R = spark.sparkContext.textFile(sys.argv[1]);
R.collect()

# S record format "<key><,><value>"
# S = spark.sparkContext.textFile("/tmp/S.txt");
S = spark.sparkContext.textFile(sys.argv[2]);
S.collect()

# rdd_S: RDD[(String, String)]
rdd_S = R.map(lambda s: s.split(",")).flatMap(lambda s: [(s[0], s[1])])
print("rdd_S=", rdd_S.collect())

# rdd_R: RDD[(String, String)]
rdd_R = S.map(lambda s: s.split(",")).flatMap(lambda s: [(s[0], s[1])])
print("rdd_R=", rdd_R.collect())

# R_joined_S: RDD[(String, (String, String))]
R_joined_S = rdd_R.join(rdd_S)
print("R_joined_S=", R_joined_S.collect())
