# RDD.mapPartitions() Example

-------

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

-------

Example of `RDD.mapPartitions()` transformation 
is given to find `(count, minimum, maximum)` for 
all given integer numbers.
 
Input data is given by a directory, which may have
any number of text files. Each input file may have 
any number of records and each record has one 
integer number.

How does algorithm work? Input is partitioned into
N chunks (i.e., `numOfPartitions = N`). From each 
partition, we find `(count_i, minimum_i, maximum_i)`
for `i = 1, 2, ..., N`.

We read input and create `source_rdd` as

	source_rdd : RDD[integer]


The we apply our custom function (called `custom_function`)
to every single partition:

~~~python
# custom_function(partition) returns  
# (count, minimum, maximum) for a given partition
# triplets : RDD[(integer, integer, integer)] = 
#            RDD[(count, minimum, maximum)]
triplets = source_rdd.mapPartitions(custom_function)
~~~
	
Finally we do one final reduction on `triplets` RDD:

~~~python
# find the final (count, minimum, maximum) for all partitions.
#
# custom_function(partition_i) creates a triplet: 
#  (count_i, minimum_i, maximum_i) for i = 1, 2, ..., N
#
# 
# Finally, we apply RDD.rduce() action to find:
#    final_result = (count, minimum, maximum)
# where
#    count = count_1 + count_2 + ...+ count_N
#    minimum = min(minimum_1, minimum_2, ..., minimum_N)
#    maximum = max(maximum_1, maximum_2, ..., maximum_N)
#
final_result = triplets.reduce(add_triplets)
~~~

The final action of `reduce()` is completed by 
the `add_triplets()`: where `add_triplets(x, y)` 
returns `(x[0]+y[0], min(x[1]+y[1]), max(x[2]+y[2]))`


~~~python
# x: (count1, minimum1, maximum1) 
# y: (count2, minimum2, maximum2) 
# returns (count1+count2, min(minimum1, minimum2), max(maximum1, maximum2))
#
def add_triplets(x, y):
  return (x[0]+y[0], min(x[1]+y[1]), max(x[2]+y[2]))
#end-def
~~~

