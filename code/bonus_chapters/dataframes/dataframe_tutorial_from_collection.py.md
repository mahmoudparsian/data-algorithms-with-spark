# Spark DataFrame Tutorial: 
# Creating Dataframes from Python Collections

	Author: Mahmoud Parsian
	
	Date: July 17, 2022

---------------

<table>
<tr>
<td>
<a href="https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/">
<img src="https://learning.oreilly.com/library/cover/9781492082378/250w/">
</a>
</td>
<td>
"... This  book  will be a  great resource for <br>
both readers looking  to  implement  existing <br>
algorithms in a scalable fashion and readers <br>
who are developing new, custom algorithms  <br>
using Spark. ..." <br>
<br>
<a href="https://cs.stanford.edu/people/matei/">Dr. Matei Zaharia</a><br>
Original Creator of Apache Spark <br>
<br>
<a href="https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/docs/FOREWORD_by_Dr_Matei_Zaharia.md">FOREWORD by Dr. Matei Zaharia</a><br>
</td>
</tr>   
</table>

----------------------

# Tutorial Description:

    This is a basic Tutorial on Spark 
    DataFrames using PySpark. It shows
    how to create Spark DataFrames from
    Python Collections.
    
---------------------------

1. Operating system command prompt begins with `$`
2. Operating system comments begin with `$#`
3. PySpark shell comments begin with `>>>#`
4. PySpark shell commands begin with `>>>`

---------------------------

# Invoke PySpark Shell

Note that /Users/mparsian/spark-3.3.0 is my 
installed Spark directory (you need to change this accordingly)


	$ cd /Users/mparsian/spark-3.3.0
	$ ./bin/pyspark
	>>>
	Welcome to
	      ____              __
	     / __/__  ___ _____/ /__
	    _\ \/ _ \/ _ `/ __/  '_/
	   /__ / .__/\_,_/_/ /_/\_\   version 3.3.0
	      /_/
	
	
	>>># spark is a SparkSession object created by PySpark shell
	>>># let's check spark
	>>> spark
	<pyspark.sql.session.SparkSession object at 0x10c85a710>
	
	>>> spark.version
	'3.3.0'
	
# Create a Python collection as `data`
	>>> # create a Python collection as data
	>>> data = 
	[
	 ('alex', 20, 12000), 
	 ('jane', 30, 45000), 
	 ('rafa', 40, 56000), 
	 ('ted', 30, 145000), 
	 ('xo2', 10, 1332000), 
	 ('mary', 44, 555000)
	]
	
	>>> # examine/display data
	>>> data
	[
	 ('alex', 20, 12000), 
	 ('jane', 30, 45000), 
	 ('rafa', 40, 56000), 
	 ('ted', 30, 145000), 
	 ('xo2', 10, 1332000), 
	 ('mary', 44, 555000)
	]

# Create a DataFrame and perform some queries

	>>># define column names
	>>> column_names = ['name', 'age', 'salary']
	
	>>> # examine/display column_names
	>>> column_names
	['name', 'age', 'salary']
	
	>>> # create a DataFrame as df from Python collection
	>>> df = spark.createDataFrame(data, column_names)
	>>>
	>>> # inspect created DataFrame
	>>> df
	DataFrame[name: string, age: bigint, salary: bigint]
	
	>>> # inspect created DataFrame's Schema
	>>> df.printSchema()
	root
	 |-- name: string (nullable = true)
	 |-- age: long (nullable = true)
	 |-- salary: long (nullable = true)
	
	>>> # display the first 20 rows of a DataFrame
	>>> df.show()
	+----+---+-------+
	|name|age| salary|
	+----+---+-------+
	|alex| 20|  12000|
	|jane| 30|  45000|
	|rafa| 40|  56000|
	| ted| 30| 145000|
	| xo2| 10|1332000|
	|mary| 44| 555000|
	+----+---+-------+
	
	>>> # count the number of rows
	>>> df.count()
	6
	
# Register your DataFrame as a Table	
	>>> # Creates or replaces a local temporary view with this DataFrame
	>>> df.createOrReplaceTempView("people")


# Run SQL queries using defined Table	

	>>> df2 = spark.sql("select * from people where salary > 67000")
	>>> df2.show()
	+----+---+-------+
	|name|age| salary|
	+----+---+-------+
	| ted| 30| 145000|
	| xo2| 10|1332000|
	|mary| 44| 555000|
	+----+---+-------+
	
	>>> df3 = spark.sql("select * from people where salary > 67000 and age > 11")
	>>> df3.show()
	+----+---+------+
	|name|age|salary|
	+----+---+------+
	| ted| 30|145000|
	|mary| 44|555000|
	+----+---+------+
	
	
	>>> df4 = spark.sql("select * from people")
	>>> df4.show()
	+----+---+-------+
	|name|age| salary|
	+----+---+-------+
	|alex| 20|  12000|
	|jane| 30|  45000|
	|rafa| 40|  56000|
	| ted| 30| 145000|
	| xo2| 10|1332000|
	|mary| 44| 555000|
	+----+---+-------+
	
	>>> # cross join: or cartesian product
	>>> cart = spark.sql("select * from people p1, people p2")
	>>> cart.show()
	+----+---+------+----+---+-------+
	|name|age|salary|name|age| salary|
	+----+---+------+----+---+-------+
	|alex| 20| 12000|alex| 20|  12000|
	|alex| 20| 12000|jane| 30|  45000|
	|alex| 20| 12000|rafa| 40|  56000|
	|alex| 20| 12000| ted| 30| 145000|
	|alex| 20| 12000| xo2| 10|1332000|
	|alex| 20| 12000|mary| 44| 555000|
	|jane| 30| 45000|alex| 20|  12000|
	|jane| 30| 45000|jane| 30|  45000|
	|jane| 30| 45000|rafa| 40|  56000|
	|jane| 30| 45000| ted| 30| 145000|
	|jane| 30| 45000| xo2| 10|1332000|
	|jane| 30| 45000|mary| 44| 555000|
	|rafa| 40| 56000|alex| 20|  12000|
	|rafa| 40| 56000|jane| 30|  45000|
	|rafa| 40| 56000|rafa| 40|  56000|
	|rafa| 40| 56000| ted| 30| 145000|
	|rafa| 40| 56000| xo2| 10|1332000|
	|rafa| 40| 56000|mary| 44| 555000|
	| ted| 30|145000|alex| 20|  12000|
	| ted| 30|145000|jane| 30|  45000|
	+----+---+------+----+---+-------+
	only showing top 20 rows
	
	>>> cart                                                                                                   
	>>> DataFrame[name: string, 
	              age: bigint, 
	              salary: bigint, 
	              name: string, 
	              age: bigint, 
	              salary: bigint]      
	>>>                                                          
	
	>>> # cross join: or cartesian product
	>>> cart2 = spark.sql("select p1.name as name, p2.age as age, p1.salary as salary, p2.name as name2, p2.age as age2, p2.salary as salary2 from people p1, people p2")
	>>> cart2.show()
	+----+---+------+-----+----+-------+
	|name|age|salary|name2|age2|salary2|
	+----+---+------+-----+----+-------+
	|alex| 20| 12000| alex|  20|  12000|
	|alex| 30| 12000| jane|  30|  45000|
	|alex| 40| 12000| rafa|  40|  56000|
	|alex| 30| 12000|  ted|  30| 145000|
	|alex| 10| 12000|  xo2|  10|1332000|
	|alex| 44| 12000| mary|  44| 555000|
	|jane| 20| 45000| alex|  20|  12000|
	|jane| 30| 45000| jane|  30|  45000|
	|jane| 40| 45000| rafa|  40|  56000|
	|jane| 30| 45000|  ted|  30| 145000|
	|jane| 10| 45000|  xo2|  10|1332000|
	|jane| 44| 45000| mary|  44| 555000|
	|rafa| 20| 56000| alex|  20|  12000|
	|rafa| 30| 56000| jane|  30|  45000|
	|rafa| 40| 56000| rafa|  40|  56000|
	|rafa| 30| 56000|  ted|  30| 145000|
	|rafa| 10| 56000|  xo2|  10|1332000|
	|rafa| 44| 56000| mary|  44| 555000|
	| ted| 20|145000| alex|  20|  12000|
	| ted| 30|145000| jane|  30|  45000|
	+----+---+------+-----+----+-------+
	only showing top 20 rows
	
	>>>
	>>> cart2
	DataFrame[name: string, age: bigint, salary: bigint, name2: string, age2: bigint, salary2: bigint]