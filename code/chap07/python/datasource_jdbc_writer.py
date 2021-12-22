from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#-----------------------------------------------------
# Create a new JDBC Table from an existing DataFrame
#
# Input Parameters: 
#       JDBC_URL
#       JDBC_DRIVER
#       JDBC_USER
#       JDBC_PASSWORD
#       JDBC_TARGET_TABLE_NAME
#------------------------------------------------------
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    #if len(sys.argv) != 2:
    #    print("Usage: datasource_jdbc_writer.py <jdbc-parameters>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    # "jdbc:mysql://localhost/metadb"    
    JDBC_URL = sys.argv[1] 
    print("JDBC_URL = ", JDBC_URL)
    #
    # "com.mysql.jdbc.Driver"
    JDBC_DRIVER = sys.argv[2] 
    print("JDBC_DRIVER = ", JDBC_DRIVER)
    #
    # "root"
    JDBC_USER = sys.argv[3]
    print("JDBC_USER = ", JDBC_USER)
    #
    # "mp22_pass"
    JDBC_PASSWORD = sys.argv[4]
    # print("JDBC_PASSWORD = ", JDBC_PASSWORD)
    #
    # "dept"
    JDBC_TARGET_TABLE_NAME = sys.argv[5]
    print("JDBC_TARGET_TABLE_NAME = ", JDBC_TARGET_TABLE_NAME)
    
    #========================================
    # Write to a JDBC table from a DataFrame
    #========================================
    
    
    # Use the SparkSession.createDataFrame() function to create a DataFrame.
    # In the following example, createDataFrame() takes a list of tuples containing
    # names, cities, and ages, and a list of column names:
    column_names = ["name", "city", "age"]
    df = spark.createDataFrame([\
        ("Alex", "Ames", 50),\
        ("Gandalf", "Cupertino", 60),\
        ("Thorin", "Sunnyvale", 95),\
        ("Betty", "Ames", 78),\
        ("Brian", "Stanford", 77)], column_names)

    #
    print("df = ", df)
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()
    df.printSchema()
    #        
        
    #-----------------------------------    
    # Write an existing DataFrame (df) 
    # to a MySQL Database Table:
    #-----------------------------------    
    df.write\
      .format('jdbc')\
      .options(\
          url=JDBC_URL,\
          driver=JDBC_DRIVER,\
          dbtable=JDBC_TARGET_TABLE_NAME,\
          user=JDBC_USER,\
          password=JDBC_PASSWORD)\
      .mode('append')\
      .save()    
      
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()