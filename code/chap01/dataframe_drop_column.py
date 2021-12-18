from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a drop() to a DataFrame:
# drops an existing column from source DataFrame
# and returns a new DataFrame
#
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------


#==========================================
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    #========================================
    # DataFrame.drop(*col)
    #
    # drop(*cols)
    # Returns a new DataFrame that drops the specified column. 
    # This is a no-op if schema doesn't contain the given column 
    # name(s).
    #
    # Parameters:	
    #    cols - a string name of the column to drop, or a 
    #           Column to drop, or a list of string name of 
    #           the columns to drop.
    #
    #========================================

    triplets = [("alex","Ames", 20),\
                ("alex", "Sunnyvale",30),\
                ("alex", "Cupertino", 40),\
                ("mary", "Ames", 35),\
                ("mary", "Stanford", 45),\
                ("mary", "Campbell", 55),\
                ("jeff", "Ames", 60),\
                ("jeff", "Sunnyvale", 70),\
                ("jane", "Austin", 80)]
                
    #
    print("triplets = ", triplets)
    df = spark.createDataFrame(triplets, ["name", "city", "age"])
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()
    df.printSchema()

    #-----------------------------------------
    # drop a column named 'city'
    #-----------------------------------------
    df2 = df.drop('city')
    df2.show()
    df2.printSchema()  
    
         
    # done!
    spark.stop()
#end-def
#=====================================
if __name__ == '__main__':
    main()

