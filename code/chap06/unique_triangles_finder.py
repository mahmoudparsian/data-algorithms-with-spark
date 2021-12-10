#!/usr/bin/python
#----------------------------------------------
# This program builds a graph using GraphFrames 
# package.  Then shows how to use "motifs" for 
# finding all Triangles. Finally, duplicate 
# triangles are dropped.
#------------------------------------------------------
# Input Parameters:
#    1) vertices: sample_graph_vertices.txt
#    2) edges: sample_graph_edges.txt
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
	.appName("find_unique_triangles")\
	.getOrCreate()

#---------------------------
# prepare vertices and edges
#---------------------------
vertices_path = sys.argv[1]
edges_path = sys.argv[2]
print("vertices_path=", vertices_path)
print("edges_path=", edges_path)

# the file representing vertices has a header 
# called "vertex_id", which will be renamed to 
# "id" (as required by GraphFrames API)
vertices_df = spark\
				.read\
				.format('csv')\
				.option('header', 'true')\
				.load(vertices_path)\
				.withColumnRenamed('vertex_id', 'id')
#                    
vertices_df.show()

#--------------------------------
# create an undirected graph, since 
# edges are directed: a -> b,
# we create two edges to make graph
# undirected:
#             a -> b
#             b -> a
# 
#------------------------------------------
# drop the "edge_weight" column, not needed
# for finding unique triangles
#------------------------------------------
raw_edges = spark\
			  .read\
			  .format('csv')\
			  .option('header', 'true')\
			  .load(edges_path)\
			  .drop("edge_weight")

# make edges undirected
edges_src_2_dst = raw_edges.selectExpr("from_id as src", "to_id as dst")
edges_dst_2_src = raw_edges.selectExpr("to_id as src", "from_id as dst")
edges_df = edges_src_2_dst.union(edges_dst_2_src)
edges_df.show()


# Once we have vertices and edges, 
# then we can build a graph:
#     vertices_df: ["id"]
#     edges_df: ["src", "dst"]
graph = GraphFrame(vertices_df, edges_df)
print("graph=", graph)

#-------------------------------
# find all triangles, which 
# might have duplicates, since
# a triangle can be represented
# in 6 different ways: given 3 
# vertices {a, b, c} of a triangle, 
# it can be represented by the 
# following 6 representations:
#
#   a -> b -> c -> a
#   a -> c -> b -> a
#   b -> a -> c -> b
#   b -> c -> a -> b
#   c -> a -> b -> c
#   c -> b -> a -> c
#-------------------------------
motifs = graph.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
print("motifs=", motifs)
print("motifs.count()=", motifs.count())
motifs.show(1000, truncate=False)

#-------------------------------
# now remove duplicate triangles
# keep only one representation of a triangle 
# {a, b, c} where a > b > c
#-------------------------------
unique_triangles = motifs[ (motifs.a > motifs.b) & (motifs.b > motifs.c)]
print("unique_triangles=", unique_triangles)
unique_triangles.show(truncate=False)

# done!
spark.stop()
