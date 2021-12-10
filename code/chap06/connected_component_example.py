#!/usr/bin/python
#-----------------------------------------------------
# This program 
#
# 1. Builds a graph using GraphFrames package.
#
# 2. Finds connected components
#
# Reference: https://en.wikipedia.org/wiki/Connected_component_(graph_theory)
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
  .appName("connected_omponent_example")\
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


#=====================
# Connected components
#=====================
# Computes the connected component membership  
# of each vertex and returns a graph with each 
# vertex assigned a component ID.
# NOTE: With GraphFrames 0.3.0 and later releases, 
# the default Connected Components algorithm requires 
# setting a Spark checkpoint directory. Users can 
# revert to the old algorithm using 
# connectedComponents.setAlgorithm("graphx").

#=====================================
# setting a Spark "checkpoint" directory
#=====================================
# What is a Checkpointing? Checkpointing is a process 
# of truncating RDD lineage graph and saving it to a 
# reliable distributed (HDFS) or local file system.
#
# You call SparkContext.setCheckpointDir(directory: String) 
# to set the checkpoint directory - the directory where RDDs 
# are checkpointed.
#
spark.sparkContext.setCheckpointDir("/tmp/spark_check_point_dir")

#==========================================
# apply the connectedComponents() algorithm
#==========================================
#
connected_components = graph.connectedComponents()
connected_components.select("id", "component").orderBy("component").show()

# done!
spark.stop()
