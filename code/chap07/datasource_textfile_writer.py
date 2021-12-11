from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

#-----------------------------------------------------
# This program writes an RDD[String] 
# onto a given output path.
#------------------------------------------------------
# Input Parameters:
#    output path
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
        print("Usage: datasource_textfile_writer.py <output-path>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # read name of input file
    output_path = sys.argv[1]
    print("output_path : ", output_path)
    #debug_file(output_path)
    

    #================================================
    # Create an RDD[String]
    #================================================
    data = ["data element 1", "data element 2", "data element 3", "data element 4"]
    print("data = " , data)
    #
    #
    records = spark.sparkContext.parallelize(data)
    #
    print("records = " , records)
    #
    print("records.count() = " , records.count())
    #
    print("records.collect() = " , records.collect())


    #================================================
    # Save an RDD[String] to an output path
    #================================================
    records.saveAsTextFile(output_path)  
    
    #================================================
    # read back from an output path and create and RDD[String]
    #================================================           
    #
    loaded_records = spark.sparkContext.textFile(output_path)
    #
    print("loaded_records = " , loaded_records)
    #
    print("loaded_records.count() = " , loaded_records.count())
    #
    print("loaded_records.collect() = " , loaded_records.collect())


    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()