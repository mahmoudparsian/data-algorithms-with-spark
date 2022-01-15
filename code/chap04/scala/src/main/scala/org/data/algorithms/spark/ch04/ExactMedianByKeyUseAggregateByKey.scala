package org.data.algorithms.spark.ch04

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * This program find median per key using
 * the aggregateByKey() transformation.
 *
 *  sourceRdd: RDD[(K, V)]
 *  targetRdd: RDD[(K, C)]
 *  C and V can be different data types
 *  targetRdd = sourceRdd.aggregateByKey(zeroValue) (seqFunction, combFunction)
 *
 * To find median(values), we use Scala's breeze package:
 *
 * scala> // Calculate middle values
 * scala> print(breeze.stats.median(Seq(1, 3, 5, 7, 9, 11, 13)))
 * 7
 * scala> print(breeze.stats.median(Seq(1, 3, 5, 7, 9, 11)))
 * 6.0
 * scala> print(breeze.stats.median(Seq(-11, 5.5, -3.4, 7.1, -9, 22)))
 * 1.05
 *------------------------------------------------------
 * Note-1: print() and collect() are used for debugging and educational purposes only.
 *
 *------------------------------------------------------
 * Note-2: groupByKey() is not very scalable for large set of values per key
 *
 *------------------------------------------------------
 * Input Parameters:
 *    none
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 *
 */
object ExactMedianByKeyUseAggregateByKey {

  def debugPartition: Iterator[(String, Int)] => Unit =
    partition => println("partition=" + partition.toList)

  def seqFunction: (Seq[Int], Int) => Seq[Int] =
    (x, y) => x.appended(y)

  def combFunction: (Seq[Int], Seq[Int]) => Seq[Int] =
    (x, y) => x.appendedAll(y)

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val input = List(("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),
      ("k2", 1), ("k2", 2), ("k2", 6), ("k2", 7), ("k2", 8),
      ("k3", 10), ("k3", 12), ("k3", 30), ("k3", 32))

    // build RDD<key, value>
    val rdd = spark.sparkContext.parallelize(input)
    rdd.foreachPartition(debugPartition)
    println("rdd.count() = " + rdd.count())
    println("rdd = " + rdd.collect().mkString("Array(", ", ", ")"))


    // group (key, value) pairs by key
    // rdd: RDD[(String, Integer)]
    // aggregatedByKey: RDD[(String, [Integer])]
    // ZERO_VALUE = []
    val aggregatedByKey = rdd.aggregateByKey(Seq.empty[Int]) (seqFunction, combFunction)


    // show aggregatedByKey
    println("aggregatedByKey = " + aggregatedByKey.collect().mkString("Array(", ", ", ")"))
    // [
    //  ('k3', [10, 12, 30, 32]),
    //  ('k2', [6, 7, 8, 1, 2]),
    //  ('k1', [1, 2, 3, 4, 5])
    // ]

    // find median per key 
    val medianPerKey = aggregatedByKey.mapValues(breeze.stats.median(_))
    println("medianPerKey = " + medianPerKey.collect().mkString("Array(", ", ", ")"))
    // avg.collect()
    // [
    //  ('k3', 21.0),
    //  ('k2', 6.0),
    //  ('k1', 3.0)
    // ]

    // done!
    spark.stop()
  }
}

/*
partition=List((k1,1))
partition=List((k3,30), (k3,32))
partition=List((k1,2), (k1,3))
partition=List((k2,6))
partition=List((k2,1), (k2,2))
partition=List((k1,4), (k1,5))
partition=List((k3,10), (k3,12))
partition=List((k2,7), (k2,8))
rdd.count() = 14
rdd = Array((k1,1), (k1,2), (k1,3), (k1,4), (k1,5), (k2,1), (k2,2), (k2,6), (k2,7), (k2,8), (k3,10), (k3,12), (k3,30), (k3,32))
aggregatedByKey = Array((k3,List(10, 12, 30, 32)), (k1,List(1, 2, 3, 4, 5)), (k2,List(1, 2, 6, 7, 8)))
medianPerKey = Array((k3,21), (k1,3), (k2,6))
 */
