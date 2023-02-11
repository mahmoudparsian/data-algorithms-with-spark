# Duplicate Removal in RDD PySpark?

How do I remove duplicates in RDD PySpark?
PySpark `RDD.distinct()	` function is used 
to drop/remove the duplicate RDD elements. 
In this article, you will learn how to use 
`distinct()` function with PySpark example.

## Duplicate Removal with distinct()

Here we use `RDD.distinct()` to remove/drop duplicate elements.

~~~python
>>> sc
<SparkContext master=local[*] appName=PySparkShell>

>>> records = ['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd = sc.parallelize(records)
>>> rdd.collect()
['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd.count()
5
>>> # drop duplicate elements
>>> # RDD.distinct() returns a new RDD containing 
>>> # the distinct elements in this RDD.
>>> 
>>> distinct_rdd = rdd.distinct()
>>> distinct_rdd.collect()
['record 3', 'record 1', 'record 2']
>>> distinct_rdd.count()
3

~~~


## Duplicate Removal without distinct()

Imagine that `RDD.distinct()` does not exist, then
how would you remove drop duplicate elements of
a given RDD.


### Solution using groupByKey()

~~~python
>>> sc
<SparkContext master=local[*] appName=PySparkShell>

>>> records = ['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd = sc.parallelize(records)
>>> rdd.collect()
['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd.count()
5
>>> # create (key, value) pairs
key_value = rdd.map(lambda x: (x, None))

>>> # group by key
>>> grouped = key_value.groupByKey()
>>> # keep the distinct key
>>> distinct_rdd = grouped.map(lambda x: x[0])
>>> 
>>> # show RDD elements
>>> distinct_rdd.collect()
['record 3', 'record 1', 'record 2']
>>> distinct_rdd.count()
3

~~~



### Solution using reduceByKey()

~~~python
>>> sc
<SparkContext master=local[*] appName=PySparkShell>

>>> records = ['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd = sc.parallelize(records)
>>> rdd.collect()
['record 1', 'record 2', 'record 1', 'record 2', 'record 3']
>>> rdd.count()
5
>>> # create (key, value) pairs
key_value = rdd.map(lambda x: (x, None))

>>> # reduce by key
>>> reduced = key_value.reduceByKey(lambda x, y: None)
>>> # keep the distinct key
>>> distinct_rdd = reduced(lambda x: x[0])
>>> 
>>> # show RDD elements
>>> distinct_rdd.collect()
['record 3', 'record 1', 'record 2']
>>> distinct_rdd.count()
3

~~~