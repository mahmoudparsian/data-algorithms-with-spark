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
object MapValuesTransformation2 {

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark =
      SparkSession.builder()
        .appName("MapValuesTransformation2")
        .master("local[*]")
        .getOrCreate()

    // Create a list of arrays.
    val listOfKeyValue= List( ("k1", List(1, 2, 3)), ("k1", List(4, 5, 6, 7)), ("k2", List(6, 7, 8)), ("k1", List(1, 3)))
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
     * find (count and average) for given set of values
     * rddMapped : RDD[(String, (Integer, Float))]
     * rddMapped : RDD[(String, (count, average))]
     *
     */
    val rddCountAvg = rdd.mapValues(v => (v.length, v.sum.toFloat / v.length))
    println("rddCountAvg = " + rddCountAvg)
    println("rddCountAvg.count() = " + rddCountAvg.count())
    println("rddCountAvg.collect() = " + rddCountAvg.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

