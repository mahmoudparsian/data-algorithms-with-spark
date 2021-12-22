#!/usr/bin/python
#-----------------------------------------------------
# This program builds a graph using GraphFrames package.
# The goal is to show how to build a graph.
#------------------------------------------------------
# Input Parameters:
#    none
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from graphframes import GraphFrame 


# create an instance of SparkSession
spark = SparkSession\
  .builder\
  .appName("graph_builder")\
  .getOrCreate()

# spark is an instance of a SparkSession
# Step-1: create vertices:
vertices = [("a", "Alice", 34),\
            ("b", "Bob", 36),\
            ("c", "Charlie", 30)]
#
v = spark.createDataFrame(vertices, ["id", "name", "age"])
v.show()
# +---+-------+---+
# | id|   name|age|
# +---+-------+---+
# |  a|  Alice| 34|
# |  b|    Bob| 36|
# |  c|Charlie| 30|
# +---+-------+---+


# Step-2: Create an Edge DataFrame with "src" and "dst" 
edges = [("a", "b", "friend"),\
         ("b", "c", "follow"),\
         ("c", "b", "follow")]
e = spark.createDataFrame(edges, ["src", "dst", "relationship"])
e.show()
# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      friend|
# |  b|  c|      follow|
# |  c|  b|      follow|
# +---+---+------------+

# Step-3: Create a GraphFrame. Using GraphFrames API, a graph 
# is built as an instance of a GraphFrame, which is a pair of 
# vertices (as `v`) and edges (as `e`):
graph = GraphFrame(v, e)
print("graph=", graph)
# GraphFrame(v:[id: string, name: string ... 1 more field], 
#            e:[src: string, dst: string ... 1 more field])


# Step-4: Query: Get in-degree of each vertex.
graph.inDegrees.show()
# +---+--------+
# | id|inDegree|
# +---+--------+
# |  c|       1|
# |  b|       2|
# +---+--------+

# The `GraphFrame.inDegrees()` return the in-degree of 
# each vertex in the graph, returned as a DataFame with 
# two columns:
#    * "id": the ID of the vertex
#    * "inDegree" (as an integer) storing the in-degree of the vertex
# Note that vertices with 0 in-edges are not returned in the result.


# Step-5: Query: Count the number of "follow" connections in the graph.
count_follow = graph.edges.filter("relationship = 'follow'").count()
print("count_follow=", count_follow)
# 2

# done!
spark.stop()
