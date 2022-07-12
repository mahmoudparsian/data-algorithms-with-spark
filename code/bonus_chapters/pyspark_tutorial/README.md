# PySpark Tutorial

* Spark is a multi-language engine for executing data engineering, 
  data science, and machine learning on single-node machines or clusters.

* PySpark is the Python API for Spark. 

Note:

* Assume that `%` is an operating system command prompt 

# Start PySpark

This section only applies if you have a cluster of 
more than one nodes. If you are going to use Spark 
on your laptop (macbook/windows), then you can ignore 
(bypass) this section.

First make sure that you have started the Spark cluster. 
To start Spark, you execute the following. Note, if you 
are going to run PySpark shell in your laptop/macbook, 
then you do not need to start any clauter -- your 
laptop/macbook as a cluster of a single node:

    % export SPARK_HOME=<installed-directory-for-spark>
	% $SPARK_HOME/sbin/start-all.sh


# Invoke  PySpark Shell

To start PySpark, execute the following:


    % export SPARK_HOME=<installed-directory-for-spark>
	% $SPARK_HOME/bin/pyspark


Successful execution will give you the PySpark prompt:


	% $SPARK_HOME/bin/pyspark
	Python 3.10.5 (v3.10.5:f377153967, Jun  6 2022, 12:36:10)
	Welcome to
		  ____              __
		 / __/__  ___ _____/ /__
		_\ \/ _ \/ _ `/ __/  '_/
	   /__ / .__/\_,_/_/ /_/\_\   version 3.3.0
		  /_/

	Using Python version 3.10.5 (v3.10.5:f377153967, Jun  6 2022 12:36:10)
	Spark context Web UI available at http://10.0.0.232:4040
	Spark context available as 'sc' (master = local[*], app id = local-1656268371486).
	SparkSession available as 'spark'.
	>>>


Note that the shell already have created two objects:
* `sc` : as parkContext object and you may use it to create RDDs.
* `spark` : an SparkSession object and you may use it to create DataFrames.


# Creating RDDs

You may create RDDs by:
* reading textfiles, 
* Python collections and data structures, 
* local file system, 
* S3 and HDFS, 
* and other data sources.


## Create RDD from a Data Structure (or Collection)

* Example-1

		>>> data = [1, 2, 3, 4, 5, 8, 9]
		>>> data
		[1, 2, 3, 4, 5, 8, 9]
		>>> myRDD = sc.parallelize(data)
		>>> myRDD.collect()
		[1, 2, 3, 4, 5, 8, 9]
		>>> myRDD.count()
		7
		>>> 


* Example-2

		>>> kv = [('a',7), ('a', 2), ('b', 2), ('b',4), ('c',1), ('c',2), ('c',3), ('c',4)]
		>>> kv
		[('a', 7), ('a', 2), ('b', 2), ('b', 4), ('c', 1), ('c', 2), ('c', 3), ('c', 4)]
		>>> rdd2 = sc.parallelize(kv)
		>>> rdd2.collect()
		[('a', 7), ('a', 2), ('b', 2), ('b', 4), ('c', 1), ('c', 2), ('c', 3), ('c', 4)]
		>>>
		>>> rdd3 = rdd2.reduceByKey(lambda x, y : x+y)
		>>> rdd3.collect()
		[('a', 9), ('c', 10), ('b', 6)]
		>>> 


* Example-3


		>>> kv = [('a',7), ('a', 2), ('b', 2), ('b',4), ('c',1), ('c',2), ('c',3), ('c',4)]
		>>> kv
		[('a', 7), ('a', 2), ('b', 2), ('b', 4), ('c', 1), ('c', 2), ('c', 3), ('c', 4)]
		>>> rdd2 = sc.parallelize(kv)
		>>> rdd2.collect()
		[('a', 7), ('a', 2), ('b', 2), ('b', 4), ('c', 1), ('c', 2), ('c', 3), ('c', 4)]

		>>> rdd3 = rdd2.groupByKey()
		>>> rdd3.collect()
		[
		 ('a', <pyspark.resultiterable.ResultIterable object at 0x104ec4c50>), 
		 ('c', <pyspark.resultiterable.ResultIterable object at 0x104ec4cd0>), 
		 ('b', <pyspark.resultiterable.ResultIterable object at 0x104ce7290>)
		]

		>>> rdd3.map(lambda x : (x[0], list(x[1]))).collect()
		[
		 ('a', [7, 2]), 
		 ('c', [1, 2, 3, 4]), 
		 ('b', [2, 4])
		]
		>>> 



# Create RDD from a Local File System (Java Example)

	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	...
	JavaSparkContext context = new JavaSparkContext();
	...
	final String inputPath ="file:///dir1/dir2/myinputfile.txt";
	JavaRDD<String> rdd = context.textFile(inputPath);
    ...


# Create RDD from HDFS (Java Example)

* Example-1:

		import org.apache.spark.api.java.JavaRDD;
		import org.apache.spark.api.java.JavaSparkContext;
		...
		JavaSparkContext context = new JavaSparkContext();
		...
		final String inputPath ="hdfs://myhadoopserver:9000/dir1/dir2/myinputfile.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
		...

* Example-2:


		import org.apache.spark.api.java.JavaRDD;
		import org.apache.spark.api.java.JavaSparkContext;
		...
		JavaSparkContext context = new JavaSparkContext();
		...
		final String inputPath ="/dir1/dir2/myinputfile.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
		...


# Create DataFrame from Python Collection
	%  $SPARK_HOME/bin/pyspark
	Python 3.10.5 (v3.10.5:f377153967, Jun  6 2022, 12:36:10) 
	Welcome to
		  ____              __
		 / __/__  ___ _____/ /__
		_\ \/ _ \/ _ `/ __/  '_/
	   /__ / .__/\_,_/_/ /_/\_\   version 3.3.0
		  /_/

	Using Python version 3.10.5 
	Spark context Web UI available at http://10.0.0.232:4041
	Spark context available as 'sc' (master = local[*], app id = local-1657575722231).
	SparkSession available as 'spark'.
	
	>>> spark.version
	'3.3.0'
	
	>>> rows = [("alex", 20, 34000), 
	            ("alex", 40, 38000),
	            ("jane", 29, 78000),
	            ("jane", 27, 37000),
	            ("ted", 50, 54000),
	            ("ted", 66, 99000)]
	>>> rows
	[
	 ('alex', 20, 34000), 
	 ('alex', 40, 38000), 
	 ('jane', 29, 78000), 
	 ('jane', 27, 37000), 
	 ('ted', 50, 54000), 
	 ('ted', 66, 99000)
	]
	
	>>> df = spark.createDataFrame(rows, ['name', 'age', 'salaray'])
	>>> df.show()
	+----+---+-------+                                                              
	|name|age|salaray|
	+----+---+-------+
	|alex| 20|  34000|
	|alex| 40|  38000|
	|jane| 29|  78000|
	|jane| 27|  37000|
	| ted| 50|  54000|
	| ted| 66|  99000|
	+----+---+-------+

	>>> df.printSchema()
	root
	 |-- name: string (nullable = true)
	 |-- age: long (nullable = true)
	 |-- salaray: long (nullable = true)

	>>> df.count()
	6
	>>> df.collect()
	[
	 Row(name='alex', age=20, salaray=34000), 
	 Row(name='alex', age=40, salaray=38000), 
	 Row(name='jane', age=29, salaray=78000), 
	 Row(name='jane', age=27, salaray=37000), 
	 Row(name='ted', age=50, salaray=54000), 
	 Row(name='ted', age=66, salaray=99000)
	]

	>>> 

# Create DataFrame from File With No Header

## Prepare input file

	cat /tmp/cats.no.header.csv
	cuttie,2,female,6
	mono,3,male,9
	fuzzy,1,female,4
	
## Create DataFrame from input file

	>>> input_path = '/tmp/cats.no.header.csv'
	>>> df = spark.read.format("csv").load(input_path)
	>>> df.show()
	+------+---+------+---+
	|   _c0|_c1|   _c2|_c3|
	+------+---+------+---+
	|cuttie|  2|female|  6|
	|  mono|  3|  male|  9|
	| fuzzy|  1|female|  4|
	+------+---+------+---+

	>>> df2 = df.withColumnRenamed('_c0', 'name')
	            .withColumnRenamed('_c1', 'count1')
	            .withColumnRenamed('_c2', 'gender')
	            .withColumnRenamed('_c3', 'count2')
	>>> df2.show()
	+------+------+------+------+
	|  name|count1|gender|count2|
	+------+------+------+------+
	|cuttie|     2|female|     6|
	|  mono|     3|  male|     9|
	| fuzzy|     1|female|     4|
	+------+------+------+------+

# Create DataFrame from File With Header

## Prepare input file

Note that the first record is the header (column names)

	cat /tmp/cats.with.header.csv
	name,count1,gender,count2
	cuttie,2,female,6
	mono,3,male,9
	fuzzy,1,female,4
	
## Create DataFrame from input file

	>>> input_path = '/tmp/cats.with.header.csv'
	>>> df = spark.read.format("csv").option("header", "true").load(input_path)
	>>> df.show()
	+------+------+------+------+
	|  name|count1|gender|count2|
	+------+------+------+------+
	|cuttie|     2|female|     6|
	|  mono|     3|  male|     9|
	| fuzzy|     1|female|     4|
	+------+------+------+------+
	
	
# Questions/Comments

* [View Mahmoud Parsian's profile on LinkedIn](http://www.linkedin.com/in/mahmoudparsian)
* Please send me an email: mahmoud.parsian@yahoo.com
* [Twitter: @mahmoudparsian](http://twitter.com/mahmoudparsian) 


Thank you!

````
best regards,
Mahmoud Parsian
````

-----


<a href="https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/">
    <img
        alt="Data Algorithms with Spark"
        src="../../../images/Data-Algorithms-with-Spark_mech2.png"
>

<a href="https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/">
    <img
        alt="Data Algorithms with Spark"
        src="../../../images/Data_Algorithms_with_Spark_COVER_9781492082385.png"
>


