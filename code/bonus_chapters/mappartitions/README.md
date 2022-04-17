# Spark's mapPartitions()

Complete [PySpark solutions](./python) are provided.

According to Spark API: ````mapPartitions(func)```` 
transformation is similar to ````map()````, but runs 
separately on each partition (block) of the RDD, so 
````func```` must be of type ````Iterator<T> => Iterator<U>````
when running on an RDD of type T. Note that `map()` operates
on each element of an RDD, while `mapPartitions()` operates
on a single partition (comprised of thousands or millions
of elements).


The ````pyspark.RDD.mapPartitions()```` transformation 
should be used when you want to extract some condensed 
(small) information (such as finding the minimum and 
maximum of numbers) from each partition. For example, 
if you want to find the minimum and maximum of all 
numbers in your input, then using ````map()```` can be 
pretty inefficient, since you will be generating tons 
of intermediate `(K, V)	` pairs, but the bottom line is 
you just want to find two numbers: the minimum and maximum 
of all numbers in your input. Another example can be if 
you want to find top-10 (or bottom-10) for your input, 
then `mapPartitions()`  can work very well: find the 
top-10 (or bottom-10) per partition, then find 
the top-10 (or bottom-10) for all partitions: this way 
you are limiting emitting too many intermediate `(K, V)`
pairs.


# Example-1: Sum Each Partition
The following is a simple example of adding numbers 
per partition:


````
>>> numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> numbers
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

>>> rdd = sc.parallelize(numbers, 3)

>>> rdd.collect()
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

>>> rdd.getNumPartitions()
3

>>> def f(iterator):
...   print ("elements: ", list(iterator))
...
>>> rdd.foreachPartition(f)
elements: 1, 2, 3
elements: 7, 8, 9, 10
elements: 4, 5, 6

>>> def adder(iterator):
...     yield sum(iterator)
...
>>> rdd.mapPartitions(adder).collect()
[6, 34, 15]

````


# Example-2: Find Minimum and Maximum

Use ````mapPartitions()```` and find the minimum and maximum from each partition.

To make it a cleaner solution, we define a python function to return the minimum and maximum for a given partition/iteration.

~~~python
# returns (min, max) per partition
def min_max(iterator):
	firsttime = True
	#local_min = 0;
	#local_max = 0;
	for x in iterator:
		if (firsttime):
			local_min = x
			local_max = x
			firsttime = False
		else:
			local_min = min(x, local_min)
			local_max = max(x, local_max)
		#end-if
	return [(local_min, local_max)]
#end-def
~~~

Now create a source RDD[String] and then apply 
`mapPartitions()`:

~~~python
>>> # spark : SparkSession object
>>> data = [10, 20, 3, 4, 5, 2, 2, 20, 20, 10]
>>> rdd = spark.sparkContext.parallelize(data, 3)
>>> mapped = rdd.mapPartitions(min_max)
>>> mapped.collect()
[(3, 20), (2, 5), (2, 20)]
>>> minmax_list = mapped.collect()
>>> minimum = min(minmax_list[0])
>>> minimum
3
>>> maximum = max(minmax_list[0])
>>> maximum
20
~~~

NOTE: data  can be huge, but for understanding 
the `mapPartitions()` we use a very small data set.


# Questions/Comments

* [View Mahmoud Parsian's profile on LinkedIn](http://www.linkedin.com/in/mahmoudparsian)
* Please send me an email: mahmoud.parsian@yahoo.com
* [Twitter: @mahmoudparsian](http://twitter.com/mahmoudparsian)

Thank you!

````
best regards,
Mahmoud Parsian
````

