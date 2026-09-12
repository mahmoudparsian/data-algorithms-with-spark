# DataFrame to RDD

There are times that you might want to 
convert a **DataFrame to an RDD**.

## RDD and DataFrame

* Spark's DataFrame (full name as: `pyspark.sql.DataFrame`)
is an immutable and distributed collection of data grouped 
into named columns.

* Spark's RDD (full name as: `pyspark.RDD`)
is a Resilient Distributed Dataset (`RDD`), 
the basic abstraction in Spark. RDD represents an 
immutable, partitioned collection of elements that 
can be operated on in parallel.

## DataFrame to RDD Conversion

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
