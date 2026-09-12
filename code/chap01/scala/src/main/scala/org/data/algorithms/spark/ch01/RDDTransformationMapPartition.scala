package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a mapPartitions() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */

/**
 * Spark's mapPartitions()
 * According to Spark API: mapPartitions(func)    transformation is
 * similar to map(), but runs separately on each partition (block)
 * of the RDD, so func must be of type Iterator<T> => Iterator<U>
 * when running on an RDD of type T.
 * The mapPartitions() transformation should be used when you want
 * to extract some condensed information (such as finding the
 * minimum and maximum of numbers) from each partition. For example,
 * if you want to find the minimum and maximum of all numbers in your
 * input, then using map() can be pretty inefficient, since you will
 * be generating tons of intermediate (K,V) pairs, but the bottom line
 * is you just want to find two numbers: the minimum and maximum of
 * all numbers in your input. Another example can be if you want to
 * find top-10 (or bottom-10) for your input, then mapPartitions()
 * can work very well: find the top-10 (or bottom-10) per partition,
 * then find the top-10 (or bottom-10) for all partitions: this way
 * you are limiting emitting too many intermediate (K,V) pairs.
 */
object RDDTransformationMapPartition {

  def debugAPartition(iterator: Iterator[Int]): Unit = {
    println("=========Begin-Parition=============")
    iterator.foreach(println)
    println("===========End-Parition=============")
  }

  def minMax(iterator: Iterator[Int]): Iterator[(Int, Int)] = {
    var firstTime = true;
    var localMin:Int = Int.MaxValue
    var localMax:Int = Int.MinValue
    iterator.foreach(x => {
      if (firstTime) {
        localMin = x
        localMax = x
        firstTime = false
      } else {
        localMin = Math.min(localMin, x)
        localMax = Math.max(localMax, x)
      }
    })
    List((localMin, localMax)).iterator
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * mapPartitions() transformation
     *
     * source_rdd.mapPartitions(function) --> target_rdd
     *
     * mapPartitions() is a 1-to-1 transformation:
     * Return a new RDD by applying a function to each partition of this RDD;
     * maps a partition into a single element of the target RDD
     *
     * mapPartitions(f, preservesPartitioning=False)[source]
     * Return a new RDD by applying a function to each partition of this RDD.
     *
     * ----------------------------------------------------
     */
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
    println("numbers = " + numbers)
    // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    // create an RDD with 3 partitions
    val rdd = spark.sparkContext.parallelize(numbers, 3)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))
    println("rdd.getNumPartitions = " + rdd.getNumPartitions)
    rdd.foreachPartition(debugAPartition)
    // Find Minimum and Maximum
    // Use mapPartitions() and find the minimum and maximum
    // from each partition.  To make it a cleaner solution,
    // we define a function to return the minimum and
    // maximum for a given iteration.
    val minmaxRdd = rdd.mapPartitions(minMax)
    println("minmaxRdd = " + minmaxRdd)
    println("minmaxRdd.count() = " + minmaxRdd.count())
    println("minmaxRdd.collect() = " + minmaxRdd.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
