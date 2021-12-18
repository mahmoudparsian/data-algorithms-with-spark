from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#-----------------------------------------------------
# Apply a describe() action to a DataFrame
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=========================================
def main():

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_action_describe.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    #========================================
    # DataFrame.describe() action
    #
    # Description:
    # Computes basic statistics for numeric and string columns.
    # This include count, mean, stddev, min, and max. If no columns 
    # are given, this function computes statistics for all numerical 
    # or string columns.
    #========================================

    pairs = [(10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9")]
    print("pairs = ", pairs)
    df = spark.createDataFrame(pairs, ["number", "name"])
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()

    #-----------------------------------------
    # apply describe() action
    #-----------------------------------------
    df.describe(['number']).show()    
    #
    df.describe().show()    

     
    # done!
    spark.stop()
#end-def
#===================================
if __name__ == '__main__':
    main()
    
