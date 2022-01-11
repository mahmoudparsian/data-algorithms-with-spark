/*
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
# @author Sanjay Bheemasenarao
#-------------------------------------------------------
*/

package org.data.algorithms.spark.ch06

import org.apache.spark.sql.SparkSession
import org.graphframes._

object BreadthFirstSearchExample {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    // spark is an instance of a SparkSession
    val spark = SparkSession
    .builder
    .appName("breadth_first_search_example").master("local[*]")
    .getOrCreate()

    // Step-1: create vertices:
    val vertices = List(("a", "Alice", 30),
    ("b", "Bob", 31),
    ("c", "Charlie", 32),
    ("d", "David", 23),
    ("e", "Emma", 24),
    ("f", "Frank", 26))

    import spark.implicits._
    val v = spark.createDataset(vertices).toDF("id", "name", "age")
    v.show()

    //Step-2: Create an Edge DataFrame with "src" and "dst"
    val edges = List(("a", "b", "follow"),
    ("b", "c", "follow"),
    ("c", "d", "follow"),
    ("d", "e", "follow"),
    ("b", "e", "follow"),
    ("c", "e", "follow"),
    ("e", "f", "follow"))

    val e = spark.createDataset(edges).toDF("src","dst","relationship")
    e.show()
    /*
     +---+---+------------+
     |src|dst|relationship|
     +---+---+------------+
     |  a|  b|      follow|
     |  b|  c|      follow|
     |  c|  d|      follow|
     |  d|  e|      follow|
     |  b|  e|      follow|
     |  c|  e|      follow|
     |  e|  f|      follow|
     +---+---+------------+
    */

    /*
    Step-3: Create a GraphFrame. Using GraphFrames API, a graph
    is built as an instance of a GraphFrame, which is a pair of
    vertices (as `v`) and edges (as `e`):
    */
    val graph = GraphFrame(v, e)
    print("graph=", graph)

    // GraphFrame(v:sql.DataFrame, e:sql.DataFrame)

    /*
     ==============
     BFS Algorithm
     ==============
    The following code snippets uses BFS to find path between
    vertex with name "Alice" to a vertex with age < 27.

    Search from "Alice" for users of age < 27.
    */

    val paths = graph.bfs.fromExpr("name = 'Alice'").toExpr("age > 30").run()
    paths.show()

    // Specify edge filters or max path lengths.
     val paths2 = graph.bfs.fromExpr("name = 'Alice'").toExpr("age > 30").edgeFilter("relationship == 'follow'").maxPathLength(4).run()
    paths2.show()

    //done!
      spark.stop()
  }
}
