#!/usr/bin/python
#-----------------------------------------------------
# This program builds a graph using GraphFrames package.
# Then shows how to apply the PageRank algorithm to the
# built graph.
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
  .appName("pagerank_example")\
  .getOrCreate()

# spark is an instance of a SparkSession
# Step-1: create vertices:
vertices = [("a", "Alice", 34),\
			("b", "Bob", 36),\
			("c", "Charlie", 30)]
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
#
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


# Step-4: Run PageRank algorithm and show results.
# Run the PageRank algorithm for the given graph for 20 iterations
# Returns: GraphFrame with new vertices column "pagerank" 
# and new edges column "weight"
pageranks = graph.pageRank(resetProbability=0.01, maxIter=20)

# show page ranks for all vertices : results
# Show the PageRank values for each node of the given graph
pageranks.vertices.select("id", "pagerank").show()
# +---+------------------+
# | id|          pagerank|
# +---+------------------+
# |  b|1.0905890109440908|
# |  a|              0.01|
# |  c|1.8994109890559092|
# +---+------------------+

# done!
spark.stop()
