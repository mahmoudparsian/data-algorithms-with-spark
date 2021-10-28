#!/usr/bin/python
#-----------------------------------------------------
# This program builds a graph using GraphFrames package.
# Then shows how to apply the Triangles Counting algorithm 
# to the built graph.
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
	.appName("count_triangles")\
	.getOrCreate()

# First, let's define vertices:
vertices = [('a', 'Alice',34),\
			('b', 'Bob', 36),\
			('c', 'Charlie',30),\
			('d', 'David',29),\
			('e', 'Esther',32),\
			('f', 'Fanny',36),\
			('g', 'Gabby',60)]

# Next, define the edges between nodes:
edges = [('a', 'b', 'friend'),\
		 ('b', 'c', 'follow'),\
		 ('c', 'b', 'follow'),\
		 ('f', 'c', 'follow'),\
		 ('e', 'f', 'follow'),\
		 ('e', 'd', 'friend'),\
		 ('d', 'a', 'friend'),\
		 ('a', 'e', 'friend')]


# Once we have vertices and edges, 
# then we can build a graph:
v = spark.createDataFrame(vertices, ["id", "name", "age"])
e = spark.createDataFrame(edges, ["src", "dst", "relationship"])
graph = GraphFrame(v, e)

# Next, we examine the built graph, vertices and edges:
print ("graph = ", graph)
# GraphFrame(
#    v:[id: string, name: string ... 1 more field], 
#    e:[src: string, dst: string ... 1 more field]
# )

v.show()
# +---+-------+---+
# | id|   name|age|
# +---+-------+---+
# |  a|  Alice| 34|
# |  b|    Bob| 36|
# |  c|Charlie| 30|
# |  d|  David| 29|
# |  e| Esther| 32|
# |  f|  Fanny| 36|
# |  g|  Gabby| 60|
# +---+-------+---+

e.show()
# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      friend|
# |  b|  c|      follow|
# |  c|  b|      follow|
# |  f|  c|      follow|
# |  e|  f|      follow|
# |  e|  d|      friend|
# |  d|  a|      friend|
# |  a|  e|      friend|
# +---+---+------------+

# Count Triangles
# To count triangles, we use the `GraphFrame.triangleCount()`
# method, which counts the number of triangles passing through 
# each vertex in this graph.
results = graph.triangleCount()
results.show()
# +-----+---+-------+---+
# |count| id|   name|age|
# +-----+---+-------+---+
# |    0|  g|  Gabby| 60|
# |    0|  f|  Fanny| 36|
# |    1|  e| Esther| 32|
# |    1|  d|  David| 29|
# |    0|  c|Charlie| 30|
# |    0|  b|    Bob| 36|
# |    1|  a|  Alice| 34|
# +-----+---+-------+---+

# To show only vertex ID and the number of triangles 
# passing through each vertex, we can write:
results.select("id", "count").show()
# +---+-----+
# | id|count|
# +---+-----+
# |  g|    0|
# |  f|    0|
# |  e|    1|
# |  d|    1|
# |  c|    0|
# |  b|    0|
# |  a|    1|
# +---+-----+

# done!
spark.stop()
