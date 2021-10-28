#!/usr/bin/python
#-----------------------------------------------------
# This program 
#
# 1. Builds a graph using GraphFrames package.
#
# 2. Applies Breadth First Search (BFS) algorithm.
#    Breadth-First Search (BFS) finds the shortest 
#    path(s) from one vertex (or a set of vertices) 
#    to another vertex (or a set of vertices). The 
#    beginning and end vertices are specified as Spark 
#    DataFrame expressions.
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
# spark is an instance of a SparkSession
spark = SparkSession\
  .builder\
  .appName("breadth_first_search_example")\
  .getOrCreate()
    
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
# |  f|  Frank| 26|
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


#==============
# BFS Algorithm
#==============
# The following code snippets uses BFS to find path between 
# vertex with name "Alice" to a vertex with age < 27.
#
# Search from "Alice" for users of age < 27.
paths = graph.bfs("name = 'Alice'", "age < 27")
paths.show()

# Specify edge filters or max path lengths.
paths2 = graph.bfs("name = 'Alice'", "age > 30",\
  edgeFilter="relationship == 'follow'", maxPathLength=4)
paths2.show()

# done!
spark.stop()
