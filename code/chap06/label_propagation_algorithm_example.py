#!/usr/bin/python
#-----------------------------------------------------
# This program 
#
# 1. Builds a graph using GraphFrames package.
#
# 2. Applies Label Propagation Algorithm (LPA)
#
# Reference: https://en.wikipedia.org/wiki/Label_Propagation_Algorithm
#
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
	.appName("label_propagation_algorithm_example")\
	.getOrCreate()

# spark is an instance of a SparkSession
# Step-1: create vertices:
vertices = [("a", "Alice", 30),\
            ("b", "Bob", 31),\
            ("c", "Charlie", 32),\
            ("d", "David", 23),\
            ("e", "Emma", 24),\
            ("f", "Frank", 26),\
            ("g", "George", 27)]
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
# |  g| George| 27|
# +---+-------+---+


# Step-2: Create an Edge DataFrame with "src" and "dst" 
#
# Build two connected components:
#     connected-component-1: {a, b, c}
#     connected-component-2: {d, e, f, g}
#
edges = [("a", "b", "follow"),\
         ("b", "c", "follow"),\
         ("d", "e", "follow"),\
         ("e", "f", "follow"),\
         ("f", "g", "follow"),\
         ("g", "d", "follow")]
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
# |  e|  f|      follow|
# |  f|  g|      follow|
# |  g|  d|      follow|
# +---+---+------------+

# Step-3: Create a GraphFrame. Using GraphFrames API, a graph 
# is built as an instance of a GraphFrame, which is a pair of 
# vertices (as `v`) and edges (as `e`):
graph = GraphFrame(v, e)
print("graph=", graph)
# GraphFrame(v:[id: string, name: string ... 1 more field], 
#            e:[src: string, dst: string ... 1 more field])


#==================================
# Label Propagation Algorithm (LPA)
#==================================
# Run static Label Propagation Algorithm for detecting 
# communities in networks.
# Each node in the network is initially assigned to 
# its own community. At every superstep, nodes send 
# their community affiliation to all neighbors and 
# update their state to the mode community affiliation 
# of incoming messages.
# 
# LPA is a standard community detection algorithm for 
# graphs. It is very inexpensive computationally, although 
# (1) convergence is not guaranteed and 
# (2) one can end up with trivial solutions 
# (all nodes are identified into a single community).
#
result = graph.labelPropagation(maxIter=5)
result.show(truncate=False)
result.select("id", "label").show(truncate=False)

# done!
spark.stop()
