import sys
from pyspark.sql import SparkSession

# create a SparkSession object
spark = SparkSession.builder.getOrCreate()


# define input path
# input_path= 's3://mybucket/INPUT2/continents_countries_temp.csv'
input_path = sys.argv[1]

# read data and create a DataFrame
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(input_path)

df.show(10, truncate=False)
df.printSchema()

# define output path
# output_path = "s3://mybucket/SCU/OUTPUT2/continents_countries1/"
output_path = sys.argv[2]

# partiton DataFrame by the "continent" column and save it to the output path
df.repartition("continent")\
    .write.mode("append")\
    .partitionBy("continent")\
    .parquet(output_path)

# done!
spark.stop()
