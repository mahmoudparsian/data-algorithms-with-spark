package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object MapPartitionsTransformation1 {
  /*
  -----------------------------------------------------
   Apply a mapPartitions() transformation to an RDD
   Input: NONE
  ------------------------------------------------------
   Input Parameters:
      file(s) containing one number per record
    -------------------------------------------------------
   @author Deepak
  -------------------------------------------------------
  */
  /*
Spark's mapPartitions()
According to Spark API: mapPartitions(func)    transformation is
similar to map(), but runs separately on each partition (block)
of the RDD, so func must be of type Iterator<T> => Iterator<U>
when running on an RDD of type T.
The mapPartitions() transformation should be used when you want
to extract some condensed information (such as finding the
minimum and maximum of numbers) from each partition. For example,
if you want to find the minimum and maximum of all numbers in your
input, then using map() can be pretty inefficient, since you will
be generating tons of intermediate (K,V) pairs, but the bottom line
is you just want to find two numbers: the minimum and maximum of
all numbers in your input. Another example can be if you want to
find top-10 (or bottom-10) for your input, then mapPartitions()
can work very well: find the top-10 (or bottom-10) per partition,
then find the top-10 (or bottom-10) for all partitions: this way
you are limiting emitting too many intermediate (K,V) pairs.
----------------------------
1. Syntax of mapPartitions()
----------------------------
Following is the syntax of PySpark mapPartitions().
It calls custom function f with argument as partition
elements and performs the function and returns all
elements of the partition. It also takes another optional
argument preservesPartitioning to preserve the partition.
    RDD.mapPartitions(f, preservesPartitioning=False)
---------------------------
2. Usage of mapPartitions()
---------------------------
def f(single_partition):
  #perform heavy initializations like Databse connections
  for element in single_partition:
    # perform operations for element in a partition
  #end-for
  # return updated data
#end-def
target_rdd = source_rdd.mapPartitions(f)
====================
==== EXAMPLE: ======
====================
In this example, we find (N, Z, P) for all given numbers, where
  N : count of all negative numbers
  Z : count of all zero numbers
  P : count of all positive numbers
*/

  def debugAPartition(iterator: Iterator[String]) = {
    var elements: ListBuffer[String] = ListBuffer()
    for (x <- iterator)
      elements += x
    println(s"elements= $elements")
  }

  def countNZP(iterator: Iterator[String]): Iterator[(Int, Int, Int)] = {
    var n, z, p, x = 0
    for (numAsString <- iterator) {
      x = numAsString.trim.toInt
      if (x < 0)
        n += 1
      else if (x > 0)
        p += 1
      else
        z += 1
    }
    return List((n, z, p)).iterator

  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Input path is missing in the command line argument")
      System.exit(-1)
    }
    //Create an instance of SparkSession
    val spark =
      SparkSession.builder()
        .appName("MapPartitionsTransformation1")
        .master("local[*]")
        .getOrCreate()
    /*
    ========================================
     mapPartitions() transformation

     source_rdd.mapPartitions(function) --> target_rdd

     mapPartitions() is a 1-to-1 transformation:
     Return a new RDD by applying a function to each
     partition of this RDD; maps a partition into a
     single element of the target RDD

     mapPartitions(f, preservesPartitioning=False)
     Return a new RDD by applying a function to each
     partition of this RDD.

    ========================================
    */
    val inputPath = args(0)
    println(s"Input Path : $inputPath")
    //create an RDD with 3 partitions
    val rdd = spark.sparkContext.textFile(inputPath, 3)
    println(s"rdd.count() = ${rdd.count()}")
    println(s"rdd.collect() = ${rdd.collect().mkString("Array(", ", ", ")")}")
    println(s"rdd.getNumPartitions() = ${rdd.getNumPartitions}")
    rdd.foreachPartition(debugAPartition)
    /*
     Find Minimum and Maximum
     Use mapPartitions() and find the minimum and maximum
     from each partition.  To make it a cleaner solution,
     we define a Scala function to return the minimum and
     maximum for a given iteration.
   */
    val nzpRDD = rdd.mapPartitions(countNZP)
    println(s"nzpRDD = $nzpRDD")
    println(s"nzpRDD.count() = ${nzpRDD.count()}")
    println(s"nzpRDD.collect() = ${nzpRDD.collect().mkString("Array(", ", ", ")")}")
    // x: denotes (N1, Z1, P1)
    // y: denotes (N2, Z2, P2)
    val finalNPZ = nzpRDD.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    println(s"final_NPZ = ${finalNPZ}")
    //Done.
    spark.stop()
  }

}
