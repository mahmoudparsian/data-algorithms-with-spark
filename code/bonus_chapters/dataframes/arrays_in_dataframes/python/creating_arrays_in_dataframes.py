from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType,StructType,StructField

#------------------------------------------
# Demo creating arrays as DataFrame columns
#------------------------------------------

spark = SparkSession.builder.getOrCreate()

list_of_strings = [ (['hello'],), (['x'],), ([],), (['Hello', 'world'],), (['I', 'am', 'fine'],) ]
#
list_schema = StructType([StructField("words",ArrayType(StringType()),True)])

df = spark.createDataFrame(list_of_strings, schema=list_schema)

df.show()

df.printSchema()

"""
>>> df.show()
+--------------+
|         words|
+--------------+
|       [hello]|
|           [x]|
|[Hello, world]|
| [I, am, fine]|
+--------------+
"""

data = [
  ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
  ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
  ("Mich,Rosen,",[],[],"NY","NJ"),
  ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
 ]
 
schema = StructType([
     StructField("name",StringType(),True),
     StructField("languagesAtSchool",ArrayType(StringType()),True),
     StructField("languagesAtWork",ArrayType(StringType()),True),
     StructField("currentState", StringType(), True),
     StructField("previousState", StringType(), True)
   ])

df2 = spark.createDataFrame(data=data,schema=schema)

df2.show()

df2.printSchema()

"""
root
 |-- name: string (nullable = true)
 |-- languagesAtSchool: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- languagesAtWork: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- currentState: string (nullable = true)
 |-- previousState: string (nullable = true)

>>> df.show()
+----------------+------------------+---------------+------------+-------------+
|            name| languagesAtSchool|languagesAtWork|currentState|previousState|
+----------------+------------------+---------------+------------+-------------+
|    James,,Smith|[Java, Scala, C++]|  [Spark, Java]|          OH|           CA|
|   Michael,Rose,|[Spark, Java, C++]|  [Spark, Java]|          NY|           NJ|
|     Mich,Rosen,|                []|             []|          NY|           NJ|
|Robert,,Williams|      [CSharp, VB]|[Spark, Python]|          UT|           NV|
+----------------+------------------+---------------+------------+-------------+
"""

