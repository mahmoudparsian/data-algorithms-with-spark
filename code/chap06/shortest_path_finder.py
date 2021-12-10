#!/usr/bin/python
#-----------------------------------------------------
# This program 
#
# 1. Builds a graph using GraphFrames package.
#
# 2. Finds Shortest paths
#    Computes shortest paths from each vertex 
#    to the given set of landmark vertices, where 
#    landmarks are specified by vertex ID. Note 
#    that this takes edge direction into account.
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
	.appName("shortest_path_finder")\
	.getOrCreate()

# spark is an instance of a SparkSession
# Step-1: create vertices:
vertices = [("a", "Alice", 30),\
			("b", "Bob", 31),\
			("c", "Charlie", 32),\
			("d", "David", 23),\
			("e", "Emma", 24),\
			("f", "Frank", 26)]
#
v = spark.createDataFrame(vertices, ["id", "name", "age"])
v.show()
# +---+-------+---+
# | id|   name|age|
# +---+-------+---+
# |  a|  Alice| 30|
# |  b|    Bob| 31|
# |  c|Charlie| 32|
# |  d|  David| 23|
# |  e|   Emma| 24|
# +---+-------+---+


# Step-2: Create an Edge DataFrame with "src" and "dst" 
edges = [("a", "b", "follow"),\
		 ("b", "c", "follow"),\
		 ("c", "d", "follow"),\
		 ("d", "e", "follow"),\
		 ("b", "e", "follow"),\
		 ("c", "e", "follow"),\
		 ("e", "f", "follow")]
#
e = spark.createDataFrame(edges, ["src", "dst", "relationship"])
e.show()
# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      follow|
# |  b|  c|      follow|
# |  c|  d|      follow|
# |  d|  e|      follow|
# |  b|  e|      follow|
# |  c|  e|      follow|
# |  e|  f|      follow|
# +---+---+------------+

# Step-3: Create a GraphFrame. Using GraphFrames API, a graph 
# is built as an instance of a GraphFrame, which is a pair of 
# vertices (as `v`) and edges (as `e`):
graph = GraphFrame(v, e)
print("graph=", graph)
# GraphFrame(v:[id: string, name: string ... 1 more field], 
#            e:[src: string, dst: string ... 1 more field])


# Computes shortest paths for landmarks ["a", "f"]
#
results = graph.shortestPaths(landmarks=["a", "f"])
print("results=", results)
#
print("results.show()=")
results.show(truncate=False)
#
print('results.select("id", "distances").show()=')
results.select("id", "distances").show()

# done!
spark.stop()
