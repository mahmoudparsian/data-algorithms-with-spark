from __future__ import print_function 
import sys 
import os
from pyspark.sql import SparkSession 
from os.path import isfile, join
#-----------------------------------------------------
# Write Content of a DataFrame to a CSV Path
# Input: Output CSV File Path
#------------------------------------------------------
# Input Parameters:
#    Output CSV File Path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=====================================
def dump_directory(dir):
    print("output dir name: ", dir)
    #contents of the current directory    
    dir_listing = os.listdir(dir) 
    print("dir_listing: ", dir_listing)
    for path in dir_listing:
        if path.startswith("part"):
            fullpath = join(dir, path)
            if isfile(fullpath) == True :          
                print("output file name: ", fullpath)
                debug_file(fullpath)
            #end-if
        #end-if
    #end-for
#end-def
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
    #    print("Usage: datasource_csv_writer.py <collection>", file=sys.stderr)
    #    exit(-1)

    # read name of Output CSV File Name
    output_csv_file_path = sys.argv[1]
    print("output_csv_file_path : ", output_csv_file_path)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()    
    
    # Write to a CSV File
    # Use the SparkSession.createDataFrame() function to create a DataFrame.
    # In the following example, createDataFrame() takes a list of tuples containing 
    # names, cities, and ages, and a list of column names:
    column_names = ["name", "city", "age"]
    people = spark.createDataFrame([\
        ("Alex", "Ames", 50),\
        ("Alex", "Sunnyvale", 51),\
        ("Alex", "Stanford", 52),\
        ("Gandalf", "Cupertino", 60),\
        ("Thorin", "Sunnyvale", 95),\
        ("Max", "Ames", 55),\
        ("George", "Cupertino", 60),\
        ("Terry", "Sunnyvale", 95),\
        ("Betty", "Ames", 78),\
        ("Brian", "Stanford", 77)], column_names)

    # Write the people DataFrame to a CSV File 
    people.write.csv(output_csv_file_path)
    #
    # check to see the content of output path
    #print("os.listdir(",output_csv_file_path, ") = ",  os.listdir(output_csv_file_path)) 
    dump_directory(output_csv_file_path)
    #
    #
    # To display the contents of the DataFrame, use the show() method.
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
#=====================================
if __name__ == '__main__':
    main()
