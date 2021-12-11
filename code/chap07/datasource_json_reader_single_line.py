from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# Create a DataFrame from a JSON file
# Input: JSON File
# In this example, there is one JSON object per line.
#------------------------------------------------------
# Input Parameters:
#    a JSON file
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
    #    print("Usage: datasource_json_reader_single_line.py <csv-file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    #

    # read name of input file
    input_path = sys.argv[1]
    print("input path : ", input_path)
    debug_file(input_path)
    

    #=====================================
    # Create a DataFrame from a given input JSON file
    #=====================================

    # Spark enable us to read JSON files 
    # and create a new DataFrame
    
    # The following example reads a JSON file  
    # and creates a new DataFrame:
    df = spark.read.json(input_path)
    #
    print("df.count() = " , df.count())
    #
    print("df.collect() = " , df.collect())
    #
    df.show(10, truncate=False)    
    #
    df.printSchema()
       
    # done!
    spark.stop()
#end-def
#=====================================

if __name__ == '__main__':
    main()
