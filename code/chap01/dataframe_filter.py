from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#-----------------------------------------------------
# Apply a filter() to a DataFrame
#
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()


    #========================================
    # filter(condition)
    # Filters rows using the given condition.
    #
    # where() is an alias for filter().
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
    #
    #
    df2 = df.filter(df.age > 50)
    print("df2.count(): ", df2.count())
    print("df2.collect(): ", df2.collect())
    df2.show()
    df2.printSchema()

    #
    #
    df3 = df.filter(df.city.contains('me'))
    print("df3.count(): ", df3.count())
    print("df3.collect(): ", df3.collect())
    df3.show()
    df3.printSchema()


    # done!
    spark.stop()
#end-def
#=====================================
if __name__ == '__main__':
    main()