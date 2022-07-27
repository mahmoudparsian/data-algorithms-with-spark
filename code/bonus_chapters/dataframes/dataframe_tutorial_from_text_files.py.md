# Spark DataFrame Tutorial: 
# Creating Dataframes from CSV Text Files

	Author: Mahmoud Parsian
	
	Date: July 17, 2022
	
-------------------

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

---------------------------

# Tutorial Description:

    This is a basic Tutorial on Spark 
    DataFrames using PySpark. It shows
    how to create Spark DataFrames from
    CSV text files.
    
    
---------------------------

1. Operating system command prompt begins with `$`
2. Operating system comments begin with `$#`
3. PySpark shell comments begin with `>>>#`
4. PySpark shell commands begin with `>>>`



# Input Data Files

	$# consider the following CSV text file (depts.csv)
	$# this file has 3 records
	$ cat /tmp/depts.csv
	d1,marketing
	d2,sales
	d3,business


	$# consider the following CSV text file (name_dept_age_salary.csv)
	$# this file has 10 records
	$ cat /tmp/name_dept_age_salary.csv
	alex,d1,60,18000
	adel,d1,40,45000
	adel,d1,50,77000
	jane,d2,40,52000
	jane,d2,60,81000
	alex,d2,50,62000
	mary,d3,50,92000
	mary,d3,60,63000
	mary,d3,40,55000
	mary,d3,40,55000

# Invoke PySpark Shell

	$# ---------------------------
	$# Invoke PySpark Shell
	$# Note that /Users/mparsian/spark-3.3.0
	$# is my installed Spark directory
	$# ---------------------------
	
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

# Define Input Paths

	>>># define input path
	>>> input_path = "/tmp/name_dept_age_salary.csv"
	>>> dept_input_path = "/tmp/depts.csv"

# Create a DataFrame from CSV Files

	>>># create a Spark DataFrame (named as emps)
	>>> emps = spark
	      .read
	      .format("csv")
	      .option("header", "false")
	      .option("inferSchema", "true")
	      .load(input_path)
	
	>>># show content of created DataFrame
	>>># note that column names are created by Spark
	>>> emps.show()
	+----+---+---+-----+
	| _c0|_c1|_c2|  _c3|
	+----+---+---+-----+
	|alex| d1| 60|18000|
	|adel| d1| 40|45000|
	|adel| d1| 50|77000|
	|jane| d2| 40|52000|
	|jane| d2| 60|81000|
	|alex| d2| 50|62000|
	|mary| d3| 50|92000|
	|mary| d3| 60|63000|
	|mary| d3| 40|55000|
	|mary| d3| 40|55000|
	+----+---+---+-----+
	
	>>># debug created emps DataFrame
	emps.printSchema
	<bound method DataFrame.printSchema of 
	 DataFrame[_c0: string, _c1: string, _c2: int, _c3: int]
	>

# Define Custom Schema for a DataFrame

	>>># Next, we define a custom schema for created DataFrame
	>>># custom schema includes column names and data types
	>>># import required libraries from Spark
	>>> from pyspark.sql.types import StructType
	>>> from pyspark.sql.types import StructField
	>>> from pyspark.sql.types import StringType
	>>> from pyspark.sql.types import IntegerType
	
	>>># create a custom schema for 4 columns
	>>># name and data types are provided
	>>> emps_schema = StructType([
	StructField("name", StringType(), True),
	StructField("dept", StringType(), True),
	StructField("age", IntegerType(), True),
	StructField("salary", IntegerType(), True)
	])

# Create a DataFrame from CSV File with Custom Schema

	>>># now, we read the data file and provide 
	>>># our custom schema (rather than using 
	>>># default schema -- provided by Spark)
	>>># emps2 DataFrame is created
	>>> emps2 = spark.read
	             .format("csv")
	             .option("header", "false")
	             .load(input_path, schema=emps_schema)  
	
	>>># examine created DataFrame emps2
	>>># column names are from our custom schema: emps_schema
	>>> emps2.show()
	+----+----+---+------+
	|name|dept|age|salary|
	+----+----+---+------+
	|alex|  d1| 60| 18000|
	|adel|  d1| 40| 45000|
	|adel|  d1| 50| 77000|
	|jane|  d2| 40| 52000|
	|jane|  d2| 60| 81000|
	|alex|  d2| 50| 62000|
	|mary|  d3| 50| 92000|
	|mary|  d3| 60| 63000|
	|mary|  d3| 40| 55000|
	|mary|  d3| 40| 55000|
	+----+----+---+------+
	
	>>># count number of rows for emps2
	>>> emps2.count()
	10
	
	>>># examine schema for emps2
	>>> emps2.printSchema
	<bound method DataFrame.printSchema of 
	DataFrame[name: string, dept: string, age: int, salary: int]
	>
	
	>>># select "dept" and  "salary" from emps2
	>>> emps2.select("dept", "salary").show()
	+----+------+
	|dept|salary|
	+----+------+
	|  d1| 18000|
	|  d1| 45000|
	|  d1| 77000|
	|  d2| 52000|
	|  d2| 81000|
	|  d2| 62000|
	|  d3| 92000|
	|  d3| 63000|
	|  d3| 55000|
	|  d3| 55000|
	+----+------+

# Register a DataFrame as a Table

	>>># Register emps2 DataFrame as a temporary 
	>>># table using the given name (employees)
	>>> emps2.registerTempTable("employees")
	
	>>># NOTE:
	>>>#      spark.sql(<sql-query>) creates a NEW 
	>>>#      DataFrame after executing your sql-query
	
	>>># select all rows
	>>> spark.sql("select * from employees").show()
	+----+----+---+------+
	|name|dept|age|salary|
	+----+----+---+------+
	|alex|  d1| 60| 18000|
	|adel|  d1| 40| 45000|
	|adel|  d1| 50| 77000|
	|jane|  d2| 40| 52000|
	|jane|  d2| 60| 81000|
	|alex|  d2| 50| 62000|
	|mary|  d3| 50| 92000|
	|mary|  d3| 60| 63000|
	|mary|  d3| 40| 55000|
	|mary|  d3| 40| 55000|
	+----+----+---+------+
	
	>>># Execute SQL query against emps2 DataFrame
	>>> spark.sql("select * from employees where salary > 55000").show()
	+----+----+---+------+
	|name|dept|age|salary|
	+----+----+---+------+
	|adel|  d1| 50| 77000|
	|jane|  d2| 60| 81000|
	|alex|  d2| 50| 62000|
	|mary|  d3| 50| 92000|
	|mary|  d3| 60| 63000|
	+----+----+---+------+
	
	>>># Find sum of salary per dept
	>>> spark.sql("select dept, sum(salary) as sum from employees group by dept").show()
	+----+------+
	|dept|   sum|
	+----+------+
	|  d2|195000|
	|  d3|265000|
	|  d1|140000|
	+----+------+


# Perform a "cross join" operation

	>>># Perform a "cross join" operation
	>>> cross_joined = spark.sql("select e1.dept, e1.age, e2.dept, e2.age from employees e1 cross join employees e2").show(100)
	+----+---+----+---+
	|dept|age|dept|age|
	+----+---+----+---+
	|  d1| 60|  d1| 60|
	|  d1| 60|  d1| 40|
	|  d1| 60|  d1| 50|
	|  d1| 60|  d2| 40|
	|  d1| 60|  d2| 60|
	|  d1| 60|  d2| 50|
	|  d1| 60|  d3| 50|
	|  d1| 60|  d3| 60|
	|  d1| 60|  d3| 40|
	|  d1| 60|  d3| 40|
	|  d1| 40|  d1| 60|
	|  d1| 40|  d1| 40|
	|  d1| 40|  d1| 50|
	|  d1| 40|  d2| 40|
	|  d1| 40|  d2| 60|
	|  d1| 40|  d2| 50|
	|  d1| 40|  d3| 50|
	|  d1| 40|  d3| 60|
	|  d1| 40|  d3| 40|
	|  d1| 40|  d3| 40|
	|  d1| 50|  d1| 60|
	|  d1| 50|  d1| 40|
	|  d1| 50|  d1| 50|
	|  d1| 50|  d2| 40|
	|  d1| 50|  d2| 60|
	|  d1| 50|  d2| 50|
	|  d1| 50|  d3| 50|
	|  d1| 50|  d3| 60|
	|  d1| 50|  d3| 40|
	|  d1| 50|  d3| 40|
	|  d2| 40|  d1| 60|
	|  d2| 40|  d1| 40|
	|  d2| 40|  d1| 50|
	|  d2| 40|  d2| 40|
	|  d2| 40|  d2| 60|
	|  d2| 40|  d2| 50|
	|  d2| 40|  d3| 50|
	|  d2| 40|  d3| 60|
	|  d2| 40|  d3| 40|
	|  d2| 40|  d3| 40|
	|  d2| 60|  d1| 60|
	|  d2| 60|  d1| 40|
	|  d2| 60|  d1| 50|
	|  d2| 60|  d2| 40|
	|  d2| 60|  d2| 60|
	|  d2| 60|  d2| 50|
	|  d2| 60|  d3| 50|
	|  d2| 60|  d3| 60|
	|  d2| 60|  d3| 40|
	|  d2| 60|  d3| 40|
	|  d2| 50|  d1| 60|
	|  d2| 50|  d1| 40|
	|  d2| 50|  d1| 50|
	|  d2| 50|  d2| 40|
	|  d2| 50|  d2| 60|
	|  d2| 50|  d2| 50|
	|  d2| 50|  d3| 50|
	|  d2| 50|  d3| 60|
	|  d2| 50|  d3| 40|
	|  d2| 50|  d3| 40|
	|  d3| 50|  d1| 60|
	|  d3| 50|  d1| 40|
	|  d3| 50|  d1| 50|
	|  d3| 50|  d2| 40|
	|  d3| 50|  d2| 60|
	|  d3| 50|  d2| 50|
	|  d3| 50|  d3| 50|
	|  d3| 50|  d3| 60|
	|  d3| 50|  d3| 40|
	|  d3| 50|  d3| 40|
	|  d3| 60|  d1| 60|
	|  d3| 60|  d1| 40|
	|  d3| 60|  d1| 50|
	|  d3| 60|  d2| 40|
	|  d3| 60|  d2| 60|
	|  d3| 60|  d2| 50|
	|  d3| 60|  d3| 50|
	|  d3| 60|  d3| 60|
	|  d3| 60|  d3| 40|
	|  d3| 60|  d3| 40|
	|  d3| 40|  d1| 60|
	|  d3| 40|  d1| 40|
	|  d3| 40|  d1| 50|
	|  d3| 40|  d2| 40|
	|  d3| 40|  d2| 60|
	|  d3| 40|  d2| 50|
	|  d3| 40|  d3| 50|
	|  d3| 40|  d3| 60|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d1| 60|
	|  d3| 40|  d1| 40|
	|  d3| 40|  d1| 50|
	|  d3| 40|  d2| 40|
	|  d3| 40|  d2| 60|
	|  d3| 40|  d2| 50|
	|  d3| 40|  d3| 50|
	|  d3| 40|  d3| 60|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 40|
	+----+---+----+---+
	
	>>># count number of rows for cross join result
	>>> cross_joined.count()
	100
	
	
	>>># cross join, but limit the same dept(s)
	>>> cross_joined_limited = spark.sql("select a.dept, a.age, b.dept, b.age from employees a, employees b where a.dept = b.dept")
	>>> cross_joined_limited.show(100)
	+----+---+----+---+
	|dept|age|dept|age|
	+----+---+----+---+
	|  d1| 60|  d1| 50|
	|  d1| 60|  d1| 40|
	|  d1| 60|  d1| 60|
	|  d1| 40|  d1| 50|
	|  d1| 40|  d1| 40|
	|  d1| 40|  d1| 60|
	|  d1| 50|  d1| 50|
	|  d1| 50|  d1| 40|
	|  d1| 50|  d1| 60|
	|  d2| 40|  d2| 50|
	|  d2| 40|  d2| 60|
	|  d2| 40|  d2| 40|
	|  d2| 60|  d2| 50|
	|  d2| 60|  d2| 60|
	|  d2| 60|  d2| 40|
	|  d2| 50|  d2| 50|
	|  d2| 50|  d2| 60|
	|  d2| 50|  d2| 40|
	|  d3| 50|  d3| 40|
	|  d3| 50|  d3| 40|
	|  d3| 50|  d3| 60|
	|  d3| 50|  d3| 50|
	|  d3| 60|  d3| 40|
	|  d3| 60|  d3| 40|
	|  d3| 60|  d3| 60|
	|  d3| 60|  d3| 50|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 60|
	|  d3| 40|  d3| 50|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 40|
	|  d3| 40|  d3| 60|
	|  d3| 40|  d3| 50|
	+----+---+----+---+

# Apply filters in SQL Queries

	>>># select all columns and filter by age
	>>> spark.sql("select * from employees where age > 40").show()
	+----+----+---+------+
	|name|dept|age|salary|
	+----+----+---+------+
	|alex|  d1| 60| 18000|
	|adel|  d1| 50| 77000|
	|jane|  d2| 60| 81000|
	|alex|  d2| 50| 62000|
	|mary|  d3| 50| 92000|
	|mary|  d3| 60| 63000|
	+----+----+---+------+
	
	>>># select 2 columns and filter by age
	>>> spark.sql("select name, age  from employees where age > 40").show()
	+----+---+
	|name|age|
	+----+---+
	|alex| 60|
	|adel| 50|
	|jane| 60|
	|alex| 50|
	|mary| 50|
	|mary| 60|
	+----+---+
	

# Creating Custom Schemas for DataFrames

	>>>
	>>># create a custom schema for a file with 2 columns
	>>> depts_schema = StructType([
	... StructField("dept", StringType(), True),
	... StructField("deptname", StringType(), True)
	... ])
	>>>
	>>># debug depts_schema
	>>> depts_schema
	StructType(List(StructField(dept,StringType,true),StructField(deptname,StringType,true)))
	>>>
	>>>

# Create a DataFrame with Custom Schema


	>>># define input path for the 2nd file:
	>>> dept_input_path = "/tmp/depts.csv"
	
	>>># read the file depts.csv and create a new DataFrame
	>>> depts = spark.read
	                 .format("csv")
	                 .option("header", "false")
	                 .load(dept_input_path, schema = depts_schema)
	>>>
	>>># show content of depts DataFrame
	>>> depts.show()
	+----+---------+
	|dept| deptname|
	+----+---------+
	|  d1|marketing|
	|  d2|    sales|
	|  d3| business|
	+----+---------+
	
	>>> depts.printSchema()
	root
	 |-- dept: string (nullable = true)
	 |-- deptname: string (nullable = true)
	
	
	>>> depts
	DataFrame[dept: string, deptname: string]
	

# Register a DataFrame as a Table

	>>># Create a temporary Table from depts DataFrame
	>>> depts.registerTempTable("departments")
	>>>
	>>># select all rows from the departments table
	>>> spark.sql("select * from departments").show()
	+----+---------+
	|dept| deptname|
	+----+---------+
	|  d1|marketing|
	|  d2|    sales|
	|  d3| business|
	+----+---------+
	
	>>># Now, use the 2 tables defined 
	>>># and find the dept name of employees
	>>> joined = spark.sql("select e.name, d.deptname from employees e, departments d where e.dept = d.dept")
	>>> joined.show()
	+----+---------+
	|name| deptname|
	+----+---------+
	|alex|marketing|
	|adel|marketing|
	|adel|marketing|
	|jane|    sales|
	|jane|    sales|
	|alex|    sales|
	|mary| business|
	|mary| business|
	|mary| business|
	|mary| business|
	+----+---------+
	
	>>># show name of dept for all employees
	>>> spark.sql("select e.name, e.dept, e.salary, d.deptname from employees e, departments d where e.dept = d.dept").show()
	+----+----+------+---------+
	|name|dept|salary| deptname|
	+----+----+------+---------+
	|alex|  d1| 18000|marketing|
	|adel|  d1| 45000|marketing|
	|adel|  d1| 77000|marketing|
	|jane|  d2| 52000|    sales|
	|jane|  d2| 81000|    sales|
	|alex|  d2| 62000|    sales|
	|mary|  d3| 92000| business|
	|mary|  d3| 63000| business|
	|mary|  d3| 55000| business|
	|mary|  d3| 55000| business|
	+----+----+------+---------+
	
	>>># perform a cross join for departments against itself
	>>> spark.sql("select d.dept, d.deptname, d2.dept, d2.deptname from departments d cross join departments d2").show()
	+----+---------+----+---------+
	|dept| deptname|dept| deptname|
	+----+---------+----+---------+
	|  d1|marketing|  d1|marketing|
	|  d1|marketing|  d2|    sales|
	|  d1|marketing|  d3| business|
	|  d2|    sales|  d1|marketing|
	|  d2|    sales|  d2|    sales|
	|  d2|    sales|  d3| business|
	|  d3| business|  d1|marketing|
	|  d3| business|  d2|    sales|
	|  d3| business|  d3| business|
	+----+---------+----+---------+
