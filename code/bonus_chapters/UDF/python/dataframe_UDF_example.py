# import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

#--------------------------------------------------
# Demo concept of Spark UDF (user-defined-function)
#--------------------------------------------------
# @author: Mahmoud Parsian
#--------------------------------------------------
def convert_case(name):
    if name is None: return None
    if len(name) < 1: return ""
    result_string = ""
    arr = name.split(" ")
    for x in arr:
       result_string += x[0:1].upper() + x[1:len(x)] + " "
    #end-for
    return result_string.strip()
#end-def
#--------------------------------------------------
def to_upper_case(name):
    if name is None: return None
    if len(name) < 1: return ""    
    return name.upper()
#end-def
#--------------------------------------------------
#
# create a SparkSession object
spark = SparkSession.builder.appName('UDF-Learning').getOrCreate()

# define column names for a DataFrame
column_names = ["ID", "Name"]

# define some rows for a DataFrame
some_data = [("100", "john jones"),
             ("200", "tracey smith"),
             ("300", "amy sanders"),
             ("400", None)]

# create a DataFrame
df = spark.createDataFrame(data=some_data,schema=column_names)

# display content of a DataFrame for testing/debugging
df.show(truncate=False)


# Converting function to UDF 
convert_case_udf = udf(lambda p: convert_case(p))

# use UDF in select stmt
df.select(col("ID"), convert_case_udf(col("Name")).alias("Name")).show(truncate=False)

# create a UDF function
upper_case_udf = udf(lambda p: to_upper_case(p), StringType())    

# Apply a UDF using withColumn
df.withColumn("Upper Name", upper_case_udf(col("Name"))).show(truncate=False)

# Using UDF on SQL 
spark.udf.register("convert_UDF", convert_case, StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select ID, convert_UDF(Name) as Name from NAME_TABLE").show(truncate=False)
     