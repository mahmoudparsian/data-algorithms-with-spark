package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * RDD.mapValues(f)
 * Pass each value in the key-value pair RDD through
 * a map function without changing the keys; this also
 * retains the original RDD’s partitioning.
 *
 * Note that mapValues() can be accomplished by map().
 *
 * Let rdd = RDD[(K, V)]
 * then the following RDDs (rdd2, rdd3) are equivalent:
 *
 *    rdd2 = rdd.mapValues(f)
 *
 *    rdd3 = rdd.map(x => (x._1, f(x._2)))
 *
 * print() is used for educational purposes.
 *------------------------------------------------------
 * Input Parameters:
 *    NONE
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object MapValuesTransformation1 {

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark =
      SparkSession.builder()
        .appName("MapValuesTransformation1")
        .master("local[*]")
        .getOrCreate()

    // Create a list of arrays.
    val listOfKeyValue= List( ("k1", (120, 12)), ("k1", (100, 5)), ("k2", (12, 2)), ("k1", (10, 5)))
    println("listOfKeyValue = " + listOfKeyValue)

    /**
     * create rdd : RDD[(String, (Integer, Integer))]
     * key: String
     * value = (Integer, Integer)
     */
    val rdd = spark.sparkContext.parallelize(listOfKeyValue)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * apply a mapValues() transformation to rdd
     *------------------------------------
     * rddMapped : RDD[(String, Integer)]
     *
     */
    val rddMapped = rdd.mapValues(v => v._1.toFloat / v._2.toFloat)
    println("rddMapped = " + rddMapped)
    println("rddMapped.count() = " + rddMapped.count())
    println("rddMapped.collect() = " + rddMapped.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
