from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#-------------------------------------------------
# Pyspark – Split multiple array columns into rows
#-------------------------------------------------
  
# creating a SparkSession object
spark=SparkSession.builder.getOrCreate()
  
# now creating dataframe
# creating the row data and giving array
# values for dataframe
sample_data = [('Rafa',  '20', ['SQL','NoSQL']),
        ('Alex',  '21', ['Ada','SQL', 'Java']),
        ('Jane',  '22', ['Fortran', 'Cobol', 'R', 'C++']),
        ('Maria', '23', [])]
  
# column names for dataframe
column_names = ['name', 'age', 'languages']
  
# creating dataframe with createDataFrame()
df = spark.createDataFrame(sample_data, column_names)
  
# printing dataframe schema
df.printSchema()
  
# show dataframe
df.show()

# using select function applying 
# explode on array column
df2 = df.select(df.name, df.age, explode(df.languages))
  
# printing the schema of the df2
df2.printSchema()
  
# show df2
df2.show()
