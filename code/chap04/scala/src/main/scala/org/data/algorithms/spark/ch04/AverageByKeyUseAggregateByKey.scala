package org.data.algorithms.spark.ch04

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * This program find average per key using
 * the aggregateByKey() transformation.
 *------------------------------------------------------
 * Input Parameters:
 *    none
 *-------------------------------------------------------
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object AverageByKeyUseAggregateByKey {

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Average By Key Use AggregateByKey")
      .master("local[*]")
      .getOrCreate()
  }

  def main(args: Array[String]) = {

    //create an instance of SparkSession
    val spark = createSparkSession()

    val input = List(("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),
      ("k2", 6), ("k2", 7), ("k2", 8),
      ("k3", 10), ("k3", 12))

    // build RDD<key, value>
    val rdd = spark.sparkContext.parallelize(input)

    /**
     * By KEY, simultaneously calculate the SUM (the numerator
     * for the average that we want to compute), and COUNT (the
     * denominator for the average that we want to compute):
     * Combined data structure (C) is a Tuple2(sum, count)
     * The following lambda functions needs to be defined:
     *
     *    zero --> C (initial-value per partition)
     *    C, v --> C (within-partition reduction)
     *    C, C --> C (cross-Partition reduction)
     *
     */
    val zero = (0.0, 0)
    /*
     * zero = (sum, count) as initial value per partition
     * C = (sum, count)
     * C1 = (sum1, count1)
     * C2 = (sum2, count2)
     * C1 + C2 = (sum1+sum2, count1+count2)
     *
     */
    val sumCount = rdd.aggregateByKey(zero)(
      (C,V) => (C._1 + V, C._2 + 1),
      (C1, C2) => (C1._1 + C2._1, C1._2 + C2._2))
    /**
     * What did happen? The following is true about the meaning of
     * each a and b pair above (so you can visualize what's happening):
     *
     * First lambda expression for Within-Partition Reduction Step::
     *   C: is a TUPLE that holds: (runningSum, runningCount).
     *   V: is a SCALAR that holds the next Value
     *
     * Second lambda expression for Cross-Partition Reduction Step::
     *   C1: is a TUPLE that holds: (runningSum, runningCount).
     *   C2: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).

     * Finally, calculate the average for each KEY, and collect results.
     * show sumCount
     */
    println("sumCount = " + sumCount.collect().mkString("Array(", ", ", ")"))

    /**
     * [
     *  ('k3', (22, 2)),
     *  ('k2', (21, 3)),
     *  ('k1', (15, 5))
     * ]
     */

    // find averages
    val avg = sumCount.mapValues( v => v._1.toFloat / v._2.toFloat)
    println("avg = " + avg.collect().mkString("Array(", ", ", ")"))
    /**
     * avg.collect()
     * [
     *  ('k3', 11),
     *  ('k2', 7),
     *  ('k1', 3)
     * ]
     */

    //done!
    spark.stop()
  }
}



