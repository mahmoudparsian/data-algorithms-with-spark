# Top 5, Top 10, Top N


![top-10](./top-10.jpeg)

--------

<table>
<tr>

<td>
<a href="https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/">
<img src="https://learning.oreilly.com/library/cover/9781492082378/250w/"></a>
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

---------
## 1. Introduction to Top 5, Top 10, Top N

	--------- sample top-10 questions ------------
	* What are the top-10 search queries? 
	* What are the top-10 movies in the past 30 years?
	* What are the top-10 business schools?
	* what are the top-10 steak houses in US?
	* what are the top-10 computer science programs in US?
	* what are the top-10 professional tennis players?
	* ...
	
Finding top 5, 10 or 20 records from a large dataset 
is the heart of many recommendation systems and it is 
also an important attribute for data analysis. 

The top 10 pattern retrieves/finds a relatively small 
number of top `N` (where `N > 0`, `N = 5, 10, 20, ...`) 
records, according to a ranking scheme in your data set, 
no matter how large the data.

This design pattern requires a comparator function ability 
between two records. That is, we must be able to compare 
one record to another to determine which is “larger.”.
Therefore, given summarized records (where `m` can be in 
thousands or millions):

	(Key_1, Count_1),
	(Key_2, Count_2),
	...
	(Key_m, Count_m)
	
where `Count_1 > Count_2 > ... > Count_10 > ... > Count_m`, 
then the top-10 list will be (note that the comparator 
function is `">"`, greater than):

	(Key_1, Count_1),
	(Key_2, Count_2),
	...
	(Key_10, Count_10)

where `Count_1 > Count_2 > ... > Count_10`.

		
How do we implement this data summarization pattern? 
This pattern utilizes both the mapper (to form a 
desired  `(key, value)` pairs), the filter (to drop 
non-desired records), the reducer (to find the 
aggregated counts), and sorter (to find final 
`top 10` list). Let `N > 0`, the mappers will find 
their local `top N`, then all of the individual 
`top N` sets will compete for the final `top N` in 
the reducer.


To understand `top N`, I am going to discuss a very 
simple problem. Consider the following data format
(for sake of simplicity, we are assuming that there 
are no `null` values -- `null` values can be removed
by a `filter()`):

	<movie_id><,><user_id><,><rating>

Further, let's assume that a user may rate the same 
movie many times (a user might watch and rate the same
movie multiple times). Suppose we want to find top-10 
movies based on the number of times movies have been 
seen by users (regardless of ratings). Let's assume that
our data was represented in table movies as:

	movies(
	        movie_id: String, 
	        user_id: String, 
	        rating: Integer
	      )

We assue that `rating` is an integer number in 
`{1, 2, 3, 4, 5}`.

## 2. Sample Data: MovieLens 25M Dataset
For finding top-10 movies, we will use MovieLens 25M Dataset:

* [MovieLens 25M Dataset: README.txt](https://files.grouplens.org/datasets/movielens/ml-25m-README.html)
* [MovieLens 25M Dataset: ml-25m.zip, size: 250 MB](https://files.grouplens.org/datasets/movielens/ml-25m.zip)


## 3. Solution in SQL

Finding top 10 in SQL is straightforward: basically
we can GROUP BY `movie_id` and `count` and then 
sort `DESC` (descending) by `count` of users. SQL's 
`LIMIT N` will give us answer for top N. In SQL,
you might be inclined to sort your data set by the 
ranking value (which is the count of movies), then 
take the top N records from that.
	
Then the following SQL will have the answer to 
our question:

	SELECT movie_id, count(user_id) as count
		FROM movies
			GROUP BY movie_id
				ORDER BY count DESC
					LIMIT 10;
					
This SQL query will provide top 10 movies seen by users
regardless of ratings. Further, if we want top 10 movies
with at least rating of 3, then our SQL query will be 
revised as  

	SELECT movie_id, count(user_id) as count
		FROM movies
			WHERE rating > 2
				GROUP BY movie_id
					ORDER BY count DESC
						LIMIT 10;				


In SQL, you may easily find bottom-10 movies as well: 
this time we sort `count` in ascending (`ASC`) order:

	SELECT movie_id, count(user_id) as count
		FROM movies
			GROUP BY movie_id
				ORDER BY count ASC
					LIMIT 10;


## 4. Solution in Spark DataFrame

Two solutions are provided using Spark's DataFrames:

1. DataFrame SQL: Create a DataFrame from input, then create 
   a table, and finally use SQL to find top-10

2. DataFrame API: Create a DataFrame from input, then use 
   DataFrame's API to find top-10

## 5. Solution in Spark RDD

[Multiple solutions](./python/) are provided by using RDDs:

1. Grouping movie IDs by using `RDD.groupByKey()`
2. Grouping movie IDs by using `RDD.reduceByKey()`
3. Grouping movie IDs by using `RDD.combineByKey()`
4. Grouping movie IDs by using `RDD.takeOrdered()`



## 6. Solution in MapReduce

Solutions in MapReduce are given in the following books:

* [MapReduce Design Patterns by Donald Miner, Adam Shook](https://www.oreilly.com/library/view/mapreduce-design-patterns/9781449341954/)

* [Data Algorithms by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms/9781491906170/)

Solutions in MapReduce use the Java's `TreeMap` data 
structure (you may use Python's dictionary libraries 
to achieve this functionality). Here, the idea is to 
use mappers to find local top 10 records, as there can 
be many mappers running parallelly on different blocks 
of data of input file(s). And then all these local top 
10 records will be aggregated at reducer where we find 
final top 10 global records for the file.

For example: assume that our input file(s) (400 GB) 
is divided into 40 blocks of 10 GB each and each block 
is processed by a mapper parallelly so we find top 10 
records (local) for that block. Then this data moves 
to the reducer where we find the actual top 10 records 
from the file movies.txt. 

## 7. Top-10 Without a Key

If your `RDD` (representation of your data) do not 
have a `(key, value)` pairs, then there are at least 
two possible solutions:


### 7.1 Using `RDD.mapPartitions()`

A simple solution will be to use `RDD.mapPartitions()` 
transformation. Let `P > 0` be the number of partitions 
of your source `RDD`, then `RDD.mapPartitions()` will 
find a local Top-10 for each partition. Therefore, if 
your source `RDD` has billions of elements, then your 
target RDD will have exactly `P` elements. Finally, 
use `RDD.reduce()` to find the final Top-10 from all 
partitioned Top-10's. General code will be 

~~~python
def custome_top_10(partition):
  # iterate partition and create a 
  # single data structure as V (as top-10)
  return [V]
#end-def

# rdd: source RDD[T] with P partitions
# rdd2 will have exacltly P elements as V_1, V_2, ..., V_P
rdd2 = rdd.mapPartitions(custome_top_10)

# apply a final RDD.reduce() to rdd2:
def custom_reducer(v1, v2):
  # reduce/merge v1, v2 into a v (as a single top-10)
  # v1: a top-10 from a partition
  # v2: a top-10 from a partition
  return v
#end-def

final_top_10 = rdd2.reduce(custome_reducer)
~~~


### 7.2 Using `RDD.takeOrdered(N)`

Another solution for an `RDD` without `(key, value)`
pairs is to use `RDD.takeOrdered(N)`, where `N` 
denotes Top-N. Note that `RDD.takeOrdered(N)` can 
be used with `(key, value)` pairs  as well.


## 8. References

1. [MapReduce example to find top n records in a sample data](https://timepasstechies.com/mapreduce-topn/)

2. [MapReduce Design Patterns by Donald Miner and Adam Shook](https://www.oreilly.com/library/view/mapreduce-design-patterns/9781449341954/)

3. [Data Algorithms by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms/9781491906170/)

4. [Data Algorithms with Spark by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/)

5. [Map Reduce for Top N Items](https://stackoverflow.com/questions/67085393/map-reduce-for-top-n-items)


## 9. Acknowledgements
I would like to say big thank you to my student: Krishna Sai Tejaswini Kambhampati
(KKambhampati@scu.edu), who has contributed to Top-N programs in PySpark.


## 10. Comments are welcome!
* Autor Contact: [ [![Email](https://support.microsoft.com/images/Mail-GrayScale.png) Email](mailto:mahmoud.parsian@yahoo.com) ]  [  [![Linkedin](https://i.stack.imgur.com/gVE0j.png) Mahmoud Parsian @LinkedIn](https://www.linkedin.com/mahmoudparsian/) ][  [![GitHub](https://i.stack.imgur.com/tskMh.png) Mahmoud Parsian @GitHub](https://github.com/mahmoudparsian/) ]
