package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Apply a groupByKey() transformation to an
 * RDD[key, value] to find average per key.
 *------------------------------------------------------
 *
 * @author: Biman Mandal
 *-------------------------------------------------------
 */
object AverageByKeyUseGroupByKey {

  def createPair(t3: (String, String, Int)) : (String, Int) = {
    // t3 = (name, city, number)
    val name = t3._1
    // city = t3._2
    val number = t3._3
    // return (k, v) pair
    (name, number)
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    /**
     * -------------------------------------------------
     * groupByKey() transformation
     *
     * source_rdd.groupByKey() --> target_rdd
     *
     * Group the values for each key in the RDD
     * into a single sequence. Hash-partitions the
     * resulting RDD with the existing partitioner/
     * parallelism level.
     *
     * Note: If you are grouping in order to perform
     * an aggregation (such as a sum or average) over
     * each key, using reduceByKey() or combineByKey()
     * will provide much better performance.
     * -------------------------------------------------
     * Create a list of tuples.
     * Each tuple contains name, city, and age.
     * Create a RDD from the list above.
     */
    val listOfTuples= List(("alex","Sunnyvale", 25),
      ("alex","Sunnyvale", 33),
      ("alex","Sunnyvale", 45),
      ("alex","Sunnyvale", 63),
      ("mary", "Ames", 22),
      ("mary", "Cupertino", 66),
      ("mary", "Ames", 20),
      ("bob", "Ames", 26))
    println("listOfTuples = " + listOfTuples)
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))
    /**
     * ------------------------------------
     *  apply a map() transformation to rdd
     *  create a (key, value) pair
     *   where
     *        key is the name (first element of tuple)
     *        value is a number
     * ------------------------------------
     */
    val rdd2 = rdd.map(createPair)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    /**
     * ------------------------------------
     *  apply a groupByKey() transformation to rdd2
     *  to create a (key, value) pairs (as rdd3)
     *   where
     *        key is the name
     *        value is the Iterable<number>
     * ------------------------------------
     */
    val rdd3 = rdd2.groupByKey()
    println("rdd3 = " + rdd3)
    println("rdd3.count() = " + rdd3.count())
    println("rdd3.collect() = " + rdd3.collect().mkString("Array(", ", ", ")"))
    // find average per key
    val averages = rdd3.mapValues(numbers => numbers.sum.toFloat / numbers.size.toFloat)
    println("averages = " + averages)
    println("averages.count() = " + averages.count())
    println("averages.collect() = " + averages.collect().mkString("Array(", ", ", ")"))
    // done!
    spark.stop()
  }
}
