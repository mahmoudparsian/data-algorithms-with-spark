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

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    #========================================
    # SparkSession.sql(sqlQuery) 
    # sql(sqlQuery)
    #
    # Description:
    # Returns a DataFrame representing the result of the given query.
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
    # Register a DataFrame as a Table
    #-----------------------------------------
    df.createOrReplaceTempView("people")
    
    #-----------------------------------------
    # Query: SELECT * FROM people
    #-----------------------------------------
    df2 = spark.sql("SELECT name, city, age FROM people")
    df2.show()  

    #-----------------------------------------
    # Query: SELECT * FROM people where age > 62
    #-----------------------------------------
    df3 = spark.sql("SELECT name, city, age FROM people WHERE age > 62 ")
    df3.show()  

    #-----------------------------------------
    # Query: SELECT name, count(*) FROM people GROUP BY "name"
    #-----------------------------------------
    df4 = df.groupBy(['name']).count()    
    df4.show()
   
    #-----------------------------------------
    # Query: SELECT name, count(*) FROM people GROUP BY "name"
    #-----------------------------------------
    df5 = spark.sql("SELECT name, count(*) as namecount FROM people GROUP BY name")   
    df5.show()
       
    # done!
    spark.stop()
    # t3 = (name, city, number)
    name = t3[0]
    #city = t3[1]
    number = int(t3[2])
    return (name, number)
#end-def
#==========================================

if __name__ == '__main__':
    main()
