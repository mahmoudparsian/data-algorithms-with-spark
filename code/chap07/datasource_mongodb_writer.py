from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# Write Content of a DataFrame to a MongoDB Collection
# Input: MongoDB Collection URI
#------------------------------------------------------
# Input Parameters:
#    MongoDB Collection URI
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    #if len(sys.argv) != 2:  
    #    print("Usage: datasource_mongodb_writer.py <collection>", file=sys.stderr)
    #    exit(-1)

    # read name of mongodb collection URI
    mongodb_collection_uri = sys.argv[1]
    print("mongodb_collection_uri : ", mongodb_collection_uri)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("datasource_mongodb_writer")\
        .config("spark.mongodb.input.uri", mongodb_collection_uri)\
        .config("spark.mongodb.output.uri", mongodb_collection_uri)\
        .getOrCreate()
    #
    print("spark=",  spark)
    
    
    # Write to MongoDB
    # Use the SparkSession.createDataFrame() function to create a DataFrame.
    # In the following example, createDataFrame() takes a list of tuples containing 
    # names, cities, and ages, and a list of column names:
    column_names = ["name", "city", "age"]
    people = spark.createDataFrame([\
        ("Alex", "Ames", 50),\
        ("Gandalf", "Cupertino", 60),\
        ("Thorin", "Sunnyvale", 95),\
        ("Betty", "Ames", 78),\
        ("Brian", "Stanford", 77)], column_names)

    # Write the people DataFrame to the MongoDB database and 
    # collection specified in the spark.mongodb.output.uri 
    # option by using the write method:
    #
    # The following operation writes to the MongoDB database and 
    # collection specified in the spark.mongodb.output.uri option.
    people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    # To read the contents of the DataFrame, use the show() method.
    people.show(10, truncate=False)
    #
    print("people.count() = " , people.count())
    #
    print("people.collect() = " , people.collect())     
    #
    # Spark samples the records to infer the schema of the collection.
    people.printSchema()
 
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
