from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a withColumn() to a DataFrame:
# add a new column to source DataFrame
# and return a new DataFrame
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
    # DataFrame.withColumn(colName, col)
    #
    # Returns a new DataFrame by adding a column 
    # or replacing the existing column that has 
    # the same name.
    #
    # The column expression must be an expression 
    # over this DataFrame; attempting to add a column 
    # from some other dataframe will raise an error.
    #
    # Parameters:	
    #   colName - string, name of the new column.
    #   col - a Column expression for the new column.
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
    # add a new column as age2
    #-----------------------------------------
    df2 = df.withColumn('age2', df.age + 2)
    df2.show()
    df2.printSchema()  
    
         
    # done!
    spark.stop()
#end-def
#==========================================
if __name__ == '__main__':
    main()
