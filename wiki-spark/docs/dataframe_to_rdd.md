# DataFrame to RDD

There are times that you might want to 
convert a **DataFrame to an RDD**.


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
