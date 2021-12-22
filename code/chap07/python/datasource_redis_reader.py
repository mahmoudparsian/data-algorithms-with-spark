#!/usr/bin/python
#-----------------------------------------------------
# Create a Redis (key, value) pairs from an existing DataFrame
#
# Input Parameters: 
#       REDIS_HOST
#       REDIS_PORT
#------------------------------------------------------
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession

def main():

    if len(sys.argv) != 3:
        print("Usage: datasource_redis_reader.py <redis-host> <redis-port>", file=sys.stderr)
        exit(-1)
        
    # redis host    
    REDIS_HOST = sys.argv[1] 
    print("REDIS_HOST = ", REDIS_HOST)
    #
    # redis port number
    REDIS_PORT = sys.argv[2] 
    print("REDIS_PORT = ", REDIS_PORT)


    # create an instance of SparkSession
    spark = SparkSession\
       .builder\
       .appName("datasource_redis_reader")\
       .config("spark.redis.host", REDIS_HOST)\
       .config("spark.redis.port", REDIS_PORT)\
       .getOrCreate()
    #
    # you may add the password, if desired
    #     .config("spark.redis.auth", "passwd")
    #
    print("spark=",  spark)


    #========================================
    # Read from Redis
    #========================================

    loaded_df = spark.read\
       .format("org.apache.spark.sql.redis")\
       .option("table", "people")\
       .option("key.column", "name")\
       .load()
    #       

    print("loaded_df = \n", loaded_df)
    print("loaded_df.count(): ", loaded_df.count())
    print("loaded_df.collect(): ", loaded_df.collect())
    loaded_df.show()
    loaded_df.printSchema()
    #     
      
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()