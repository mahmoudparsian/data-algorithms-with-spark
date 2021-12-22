from __future__ import print_function
import sys
from pyspark.sql import SparkSession

#-----------------------------------------------------
# Read a JDBC Table and Create a DataFrame
#
# Input Parameters: 
#       JDBC_URL
#       JDBC_DRIVER
#       JDBC_USER
#       JDBC_PASSWORD
#       JDBC_TABLE_NAME
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    #if len(sys.argv) != 2:
    #    print("Usage: dataframe_filter.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    #

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
    # "mp22_password"
    JDBC_PASSWORD = sys.argv[4]
    # print("JDBC_PASSWORD = ", JDBC_PASSWORD)
    #
    # "dept"
    JDBC_SOURCE_TABLE_NAME = sys.argv[5]
    print("JDBC_SOURCE_TABLE_NAME = ", JDBC_SOURCE_TABLE_NAME)
    
    #========================================
    # Read a JDBC table and create a DataFrame
    #========================================

    df = spark\
        .read\
        .format("jdbc")\
        .option("url", JDBC_URL)\
        .option("driver", JDBC_DRIVER)\
        .option("user", JDBC_USER)\
        .option("password", JDBC_PASSWORD)\
        .option("dbtable", JDBC_SOURCE_TABLE_NAME)\
        .load()
    #
    print("df = ", df)
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()
    df.printSchema()

    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()