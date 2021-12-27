package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 * -------------------------------------------------------
 * Apply a mapPartitions() transformation to an RDD
 * Input: NONE
 * -------------------------------------------------------
 * Input Parameters:
 * None
 * ------------------------------------------------------
 *
 * @author Deepak Kumar
 *         -------------------------------------------------------
 *
 *
 *         Find (minimum, maximum, count) by
 *         using mapPartitions() transformation
 *         Spark's mapPartitions()
 *         According to Spark API: mapPartitions(func)    transformation is
 *         similar to map(), but runs separately on each partition (block)
 *         of the RDD, so func must be of type Iterator<T> => Iterator<U>
 *         when running on an RDD of type T.
 *         The mapPartitions() transformation should be used when you want
 *         to extract some condensed information (such as finding the
 *         minimum and maximum of numbers) from each partition. For example,
 *         if you want to find the minimum and maximum of all numbers in your
 *         input, then using map() can be pretty inefficient, since you will
 *         be generating tons of intermediate (K,V) pairs, but the bottom line
 *         is you just want to find two numbers: the minimum and maximum of
 *         all numbers in your input. Another example can be if you want to
 *         find top-10 (or bottom-10) for your input, then mapPartitions()
 *         can work very well: find the top-10 (or bottom-10) per partition,
 *         then find the top-10 (or bottom-10) for all partitions: this way
 *         you are limiting emitting too many intermediate (K,V) pairs.
 *         =================
 *         Empty Partitions:
 *         =================
 *         This example shows how to handle Empty Partitions.
 *         An empty partition is a partition, which has no
 *         elements in it. Empty partions should be handled gracefully.
 */

object RDDTransformationMappartitionsHandleEmptyPartitions {

  //=========================================
  def debugAPartition(iterator: Iterator[String]): Unit = {
    print("==begin-partition=")
    for (x <- iterator)
      println(x)
    //end-for
    println("==end-partition=")
    //end-def
  }
  //=========================================

  //==========================================
  // iterator : a pointer to a single partition
  // (min, max) will be returned for a single partition
  // <1> iterator is a type of 'itertools.chain'
  // <2> print the type of iterator (for debugging only)
  // <3> try to get the first record from a given iterator,
  // if successful, then the first_record is initialied to
  // the first record of a partition
  // <4> if you are here, then it means that the partition
  // is empty, return a fake triplet ((1,-1,0))
  // <5> set min, max, and count from the first record
  // <6> iterate the iterator for 2nd, 3rd, ... records
  // (record holds a single record)
  // <7> finally return a triplet from each partition

  def min_max_count(iterator: Iterator[String]): Iterator[(Int, Int, Int)] = {
    var firstRecord: String = ""
    try {
      firstRecord = iterator.next()
    } catch {
      case e: NoSuchElementException => return List((1, -1, 0)).iterator
    }
    var numbers = firstRecord.
      split(",").map(_.toInt)
    var local_min = numbers.min
    var local_max = numbers.max
    var local_count = numbers.length
    //
    for (record <- iterator) {
      numbers = record.split(",").map(_.toInt)
      var min2 = numbers.min
      var max2 = numbers.max
      local_count += numbers.length
      //
      local_max = Math.max(max2, local_max)
      local_min = Math.min(min2, local_min)

    } //end for
    return List((local_min, local_max, local_count)).iterator
  }
  //end-def

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    //========================================
    // mapPartitions() transformation
    //
    // source_rdd.mapPartitions(function) --> target_rdd
    //
    // mapPartitions() is a 1-to-1 transformation:
    // Return a new RDD by applying a function to each partition of this RDD;
    // maps a partition into a single element of the target RDD
    //
    // mapPartitions(f, preservesPartitioning=False)[source]
    // Return a new RDD by applying a function to each partition of this RDD.
    //
    //========================================
    val numbers = List("10,20,3,4",
      "3,5,6,30,7,8",
      "4,5,6,7,8",
      "3,9,10,11,12",
      "6,7,13",
      "5,6,7,12",
      "5,6,7,8,9,10",
      "11,12,13,14,15,16,17"
    )
    println(numbers)
    //create an RDD with 10 partitions
    //with high number of partitions,
    //some of the partitions will be Empty
    val rdd = spark.sparkContext.parallelize(numbers, 10)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect())
    println("rdd.getNumPartitions() = " + rdd.getNumPartitions)
    rdd.foreachPartition(debugAPartition)
    // Find Minimum and Maximum
    // Use mapPartitions() and find the minimum and maximum
    // from each partition.  To make it a cleaner solution,
    // we define a function to return the minimum and
    // maximum for a given iteration.
    val min_max_count_rdd = rdd.mapPartitions(min_max_count)
    println("min_max_count_rdd = " + min_max_count_rdd)
    println("min_max_count_rdd.count() = " + min_max_count_rdd.count())
    println("min_max_count_rdd.collect() = " + min_max_count_rdd.collect())
    min_max_count_rdd.foreach(println _)

    val min_max_count_list = min_max_count_rdd.collect()
    println("min_max_count_list=" + min_max_count_list)

    val min_max_count_filtered = min_max_count_rdd.filter(x => x._1 <= x._2)
    //now finalize the (min, max, count)
    val min_max_count_filtered_list = min_max_count_filtered.collect()
    min_max_count_filtered_list.foreach(println("min_max_count_filtered_list =", _))

    //==============================
    // final final (min, max, count)
    //==============================
    var firstTime = true
    var final_min = 0
    var final_max = 0
    var final_count = 0

    for (t3 <- min_max_count_filtered_list)
      if (firstTime) {
        final_min = t3._1
        final_max = t3._2
        final_count = t3._3
        firstTime = false
      }
      else {
        final_count += t3._3
        final_max = Math.max(final_max, t3._2)
        final_min = Math.min(final_min, t3._1)
      } //emd-if
    //end-for
    println("final_min = " + final_min)
    println("final_max = " + final_max)
    println("final_count = " + final_count)

    //done!
    spark.stop()

  } //end-main
}
