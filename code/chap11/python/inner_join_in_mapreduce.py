"""
Join in MapReduce

@author: Mahmoud Parsian

This is a custom implementation of inner-join
without using Spark's join() functions.


This section is presented for pedagogical purposes, 
to show you how a join() function can be implemented 
in a distributed computing environment. Suppose we have
two relations, R(k, b) and S(k, c), where k is a common 
key  and  b  and  c represent attributes of R and S, 
respectively.  How do we find the join of R and S? The 
goal of the join operation is to find tuples that agree 
on their key k.  A MapReduce implementation of the natural 
join for R and S can implemented as follows. First, in 
the map phase:

-> For a tuple (k, b) in R, emit a (key, value) pair as (k, ("R", b)).
-> For a tuple (k, c) in S, emit a (key, value) pair as (k, ("S", c)).
Then, in the reduce phase:
-> If a reducer key k has the value list [("R", v),("S", w)], then 
emit a single (key, value) pair as (k, (v, w)). Note that join(R, S) 
will produce (k, (v, w)), while join(S, R) will produce (k, (w, v)).
So, if a reducer key k has the value list 
[("R", v1), ("R", v2), ("S", w1), ("S", w2)], 
then we will emit four (key, value) pairs:
(k, (v1, w1))
(k, (v1, w2))
(k, (v2, w1))
(k, (v2, w2))

Therefore, to perform a natural join between two relations R and S, 
we need two map functions and one reducer function.

Map Phase:
----------
The map phase has two steps:
1. Map relation R:
# key: relation R
# value: (k, b) tuple in R
map(key, value) {
   emit(k, ("R", b))
}

2. Map relation S:
# key: relation S
# value: (k, c) tuple in S
map(key, value) {
   emit(k, ("S", c))
}

The output of the mappers (provided as input to the sort 
and shuffle phase) will be:

(k1, "R", r1)
(k1, "R", r2)
...
(k1, "S", s1)
(k1, "S", s2)
...
(k2, "R", r3)
(k2, "R", r4)
...
(k2, "S", s3)
(k2, "S", s4)
...

Reducer Phase:
--------------
Before, we write a reducer function, we need to understand 
the magic of MapReduce, which occurs in the sort and shuffle 
phase. This is similar to SQL's GROUP BY function; once all 
the mappers are done, their output is sorted and shuffled 
and sent to thereducer(s) as input.   In our example, the 
output of the sort and shuffle phase will be:

(k1, [("R", r1), ("R", r2), ..., ("S", s1), ("S", s2), ...]
(k2, [("R", r3), ("R", r4), ..., ("S", s3), ("S", s4), ...]
...

The reducer function is presented next. For each key k, we 
build two lists: list_R (which will hold the values/attributes 
from relation R) and list_S (which will hold the values/attributes 
from relation S). Then we identify the Cartesian product of list_R 
and list_S to find the join tuples:

# key: a unique key
# values: [(relation, attrs)] where relation in {"R", "S"}
# and attrs are the relation attributes
reduce(key, values) {
  list_R = []
  list_S = []
  for (tuple in values) {
    relation = tuple[0]
    attributes = tuple[1]
    if (relation == "R") {
      list_R.append(attributes)
    }
    else {
      list_S.append(attributes)
    }
  }

  # check to see if there are common keys
  # between two tables
  if (len(list_R) == 0) OR (len(list_S) == 0) {
    # no common key
    return
  }
  
  # len(list_R) > 0 AND len(list_S) > 0
  # perform Cartesian product of list_R and list_S
  for (r in list_R) {
    for (s in list_S) {
      emit(key, (r, s))
    }
  }
}

Implementation in PySpark:
--------------------------
This section shows how to implement the natural join of 
two datasets (with some common keys) in PySpark without 
using the join() function. I present this solution to show 
the power of Spark, and how it can be used to perform custom 
joins if required. Suppose we have the following datasets, 
T1 and T2:
"""

from __future__ import print_function
import sys
import itertools
#
from pyspark.sql import SparkSession
from pyspark.sql import Row
#
#---------------------------------------------------------
# key: entry[0]: key
# values: entry[1]: [("T1", t11), ("T1", t12), ..., ("T2", t21), ("T2", t22), ...]
def cartesian_product(entry):
  T1 = []
  T2 = []
  key = entry[0]
  values = entry[1]
  for tuple in values:
    relation = tuple[0]
    attributes = tuple[1]
    if (relation == "T1"): 
      T1.append(attributes)
    else: 
      T2.append(attributes)
    #end-if
  #end-for

  # check to see if there are common keys?
  if (len(T1) == 0) or (len(T2) == 0):
    # no common key
    return []
  # assertion: len(T1) > 0 AND len(T2) > 0
  joined_elements = []
  # perform Cartesian product of T1 and T2
  #for v in T1:
  # for w in T2:
  #  joined_elements.append((key, (v, w)))
  for element in itertools.product(T1, T2):
    joined_elements.append((key, element))
  #end-for
  #
  return joined_elements
#end-def
#---------------------------------------------------------

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()

d1 = [('a', 10), ('a', 11), ('a', 12), ('b', 100), ('b', 200), ('c', 80)]
d2 = [('a', 40), ('a', 50), ('b', 300), ('b', 400), ('d', 90)]
T1 = spark.sparkContext.parallelize(d1)
T2 = spark.sparkContext.parallelize(d2)

# First, we map these RDDs to include the name of the relation:
t1_mapped = T1.map(lambda x: (x[0], ("T1", x[1])))
t2_mapped = T2.map(lambda x: (x[0], ("T2", x[1])))

# Next, in order to perform a reduction of the same key, 
# we combine these two datasets into the dataset:
combined = t1_mapped.union(t2_mapped)

# Then we perform the groupByKey() transformation 
# on a single combined dataset:
grouped = combined.groupByKey()

# And finally, we find the Cartesian product of the 
# values of each grouped entry:
joined = grouped.flatMap(cartesian_product)
print("joined=", joined.collect())

"""
sample run and output:

$SPARK_HOME/bin/spark-submit join_in_mapreduce.py
joined= 
[
 ('b', (100, 300)), 
 ('b', (100, 400)), 
 ('b', (200, 300)), 
 ('b', (200, 400)), 
 ('a', (10, 40)), 
 ('a', (10, 50)), 
 ('a', (11, 40)), 
 ('a', (11, 50)), 
 ('a', (12, 40)), 
 ('a', (12, 50))
]

"""
