from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# Create a DataFrame from a CSV file Without a Header
# Input: CSV File Without a Header
#------------------------------------------------------
# Input Parameters:
#    a CSV file Without a Header
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=====================================
def debug_file(input_path):
    # Opening a file in python for reading is easy:
    f = open(input_path, 'r')

    # To get everything in the file, just use read()
    file_contents = f.read()
    
    #And to print the contents, just do:
    print ("file_contents = \n" + file_contents)

    # Don't forget to close the file when you're done.
    f.close()
#end-def
#=====================================
def main():

    #if len(sys.argv) != 2:  
    #    print("Usage: datasource_csv_reader_no_header.py <csv-file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    # read name of input file
    input_path = sys.argv[1]
    print("input path : ", input_path)
    debug_file(input_path)
    

    #=====================================
    # Create a DataFrame from a given input file
    #=====================================

    # Spark enable us to read CSV files with or without a header.
    # Heare we will read a CSV file without a header and create
    # a new DataFrame
    
    # The following example reads a CSV file without a 
    # header and create a new DataFrame and infers a 
    # schema from the content of columns:
    df = spark\
          .read\
          .format("csv")\
          .option("header","false")\
          .option("inferSchema", "true")\
          .load(input_path)
    #
    print("df = " , df.collect())
    #
    df.show()    
    #
    df.printSchema()


    # You may rename column names of a DataFrame
    # change default column names to your desired column names
    df2 = df.selectExpr("_c0 as name", "_c1 as city", "_c2 as age")
    #
    df2.show()
    #
    df2.printSchema()
       
    # done!
    spark.stop()
#end-def
#=====================================

if __name__ == '__main__':
    main()

