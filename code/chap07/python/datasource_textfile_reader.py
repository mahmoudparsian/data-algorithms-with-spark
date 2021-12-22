from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# This program reads a text file of numbers
# (each record may have one or more numbers
# separated by ",") and creates an RDD[String]
# (ealch element is a record of text file read)
# and then transforms it to RDD[Integer] (one
# interger as an element)
#------------------------------------------------------
# Input Parameters:
#    Text File(s)
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

    if len(sys.argv) != 2:  
        print("Usage: datasource_textfile_reader.py <text-file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # read name of input file
    input_path = sys.argv[1]
    print("input_path : ", input_path)
    debug_file(input_path)
    

    #================================================
    # Create an RDD[String] from a given text file(s)
    #================================================
    records = spark.sparkContext.textFile(input_path)
    #
    print("records = " , records)
    #
    print("records.count() = " , records.count())
    #
    print("records.collect() = " , records.collect())


    #================================================
    # Transform an RDD[String] to RDD[Integer]
    #================================================
    numbers = records.flatMap(lambda rec: [int(n) for n in rec.split(",")])         
    #
    print("numbers = " , numbers)
    #
    print("numbers.count() = " , numbers.count())
    #
    print("numbers.collect() = " , numbers.collect())


    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()
