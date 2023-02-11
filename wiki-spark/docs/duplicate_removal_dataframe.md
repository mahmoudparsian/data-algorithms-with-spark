# Duplicate Removal in DataFrame PySpark?

How do I remove duplicates in DataFrame PySpark?
PySpark `distinct()` function is used to drop/remove 
the duplicate rows (all columns) from DataFrame and 
`dropDuplicates()` is used to drop rows based on 
selected (one or multiple) columns. In this article, 
you will learn how to use `distinct()` function with 
PySpark example.

## Duplicate Removal with `distinct()` and `dropDuplicates()`

~~~python
>>> spark
<pyspark.sql.session.SparkSession object at 0x10cba26e0>
>>> spark.version
'3.3.1'

>>> records = [('alex', 10, 50), ('alex', 10, 50), ('alex', 10, 80), ('jane', 20, 90), ('jane', 20, 90), ('ted', 30, 70)]

>>> column_names = ["name", "age", "height"]
>>> df = spark.createDataFrame(records, column_names)
>>> df.show()
+----+---+------+
|name|age|height|
+----+---+------+
|alex| 10|    50|
|alex| 10|    50|
|alex| 10|    80|
|jane| 20|    90|
|jane| 20|    90|
| ted| 30|    70|
+----+---+------+


>>> df2 = df.distinct()
>>> df2.show()
+----+---+------+
|name|age|height|
+----+---+------+
|alex| 10|    50|
|alex| 10|    80|
|jane| 20|    90|
| ted| 30|    70|
+----+---+------+

>>> df.dropDuplicates().show()
+----+---+------+
|name|age|height|
+----+---+------+
|alex| 10|    50|
|alex| 10|    80|
|jane| 20|    90|
| ted| 30|    70|
+----+---+------+

>>> df.dropDuplicates(['name', 'age']).show()
+----+---+------+
|name|age|height|
+----+---+------+
|alex| 10|    50|
|jane| 20|    90|
| ted| 30|    70|
+----+---+------+

~~~