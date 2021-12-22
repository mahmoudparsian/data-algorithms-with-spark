from __future__ import print_function
import sys
from pyspark.sql import SparkSession

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
def main():

    if len(sys.argv) != 3:
        print("Usage: datasource_redis_writer.py <redis-host> <redis-port>", file=sys.stderr)
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
       .appName("datasource_redis_writer")\
       .config("spark.redis.host", REDIS_HOST)\
       .config("spark.redis.port", REDIS_PORT)\
       .getOrCreate()
    #
    # you may add the password, if desired
    #     .config("spark.redis.auth", "passwd")
    #
    print("spark=",  spark)


    #========================================
    # Write to Redis from a DataFrame
    #========================================
    
    
    # Use the SparkSession.createDataFrame() function 
    # to create a DataFrame. In the following example, 
    # createDataFrame() takes a list of tuples containing  
    # names, cities, and ages, and a list of column names:
    #
    column_names = ["name", "city", "age"]
    #
    df = spark.createDataFrame([\
        ("Alex", "Ames", 50),\
        ("Gandalf", "Cupertino", 60),\
        ("Thorin", "Sunnyvale", 95),\
        ("Betty", "Ames", 78),\
        ("Brian", "Stanford", 77)], column_names)

    #
    print("df = \n", df)
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()
    df.printSchema()
    #        
        
    #-----------------------------------    
    # Write an existing DataFrame (df) 
    # to a redis database:
    #-----------------------------------    
    df.write\
        .format("org.apache.spark.sql.redis")\
        .option("table", "people")\
        .option("key.column", "name")\
        .save()
        

    #-----------------------------------    
    # READ back data from Redis:
    #-----------------------------------    
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