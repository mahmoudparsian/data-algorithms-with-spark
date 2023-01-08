# RDD to DataFrame & DataFrame to RDD

There are times that you might want to 
convert an **RDD to a DataFrame** and 
a **DataFrame to an RDD**.

## RDD to DataFrame

To map an `RDD` to a `DataFrame`, you need to convert
every element of your source `RDD` to a `pyspark.sql.Row`
object.

	>>> spark.version
	'3.3.1'
	
	>>> records = [("alex", 10), ("jane", 20), ("rose", 30)]
	>>> rdd = spark.sparkContext.parallelize(records)
	>>> rdd.collect()
	[('alex', 10), ('jane', 20), ('rose', 30)]
	
	>>># Convert each element of an RDD to a Row object
	>>> from pyspark.sql import Row
	>>> rows = rdd.map(lambda x: Row(name=x[0], age=x[1]))
	>>> rows.collect()
	[
	 Row(name='alex', age=10), 
	 Row(name='jane', age=20), 
	 Row(name='rose', age=30)
	]
	>>> df = rdd.toDF()
	>>> df.show()
	+----+---+
	|  _1| _2|
	+----+---+
	|alex| 10|
	|jane| 20|
	|rose| 30|
	+----+---+

	>>> df = rdd.toDF(["name", "age"])
	>>> df.show()
	+----+---+
	|name|age|
	+----+---+
	|alex| 10|
	|jane| 20|
	|rose| 30|
	+----+---+


## DataFrame to RDD
To convert a `DataFrame` to an `RDD`, you just need to
call `DataFrame.rdd`.

	>>> spark.version
	'3.3.1'
	>>> records = [("alex", 10), ("jane", 20), ("rose", 30)]
	>>> df = spark.createDataFrame(records, ["name", "age"])
	>>> df.show()
	+----+---+
	|name|age|
	+----+---+
	|alex| 10|
	|jane| 20|
	|rose| 30|
	+----+---+
	
	>>># Convert a DataFrame to an RDD
	>>> rdd = df.rdd
	>>> rdd.collect()
	[
	 Row(name='alex', age=10), 
	 Row(name='jane', age=20), 
	 Row(name='rose', age=30)
	]
