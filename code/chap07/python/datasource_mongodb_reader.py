from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# Read MongoDB Collection and create a DataFrame
# Input: MongoDB Collection URI
#------------------------------------------------------
# Input Parameters:
#    MongoDB Collection URI
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    #if len(sys.argv) != 2:  
    #    print("Usage: datasource_mongodb_reader.py <collection>", file=sys.stderr)
    #    exit(-1)

    # read name of mongodb collection URI
    mongodb_collection_uri = sys.argv[1]
    print("mongodb_collection_uri : ", mongodb_collection_uri)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("datasource_mongodb_reader")\
        .config("spark.mongodb.input.uri", mongodb_collection_uri)\
        .config("spark.mongodb.output.uri", mongodb_collection_uri)\
        .getOrCreate()
    #
    print("spark=",  spark)

    # Assign the collection to a DataFrame with spark.read() 
    # from within the pyspark shell.
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    #
    print("df.count() = " , df.count())
    #
    print("df.collect() = " , df.collect())
    #
    df.show(10, truncate=False)        
    #
    # Spark samples the records to infer the schema of the collection.
    df.printSchema()
 
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
