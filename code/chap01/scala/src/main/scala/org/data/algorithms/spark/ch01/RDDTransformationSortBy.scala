package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a sortByKey() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDTransformationSortBy {

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * sortByKey() transformation
     *
     * sortByKey(ascending=true)
     *
     * Description:
     * Sorts this RDD, which is assumed to consist of (key, value) pairs.
     *-----------------------------------------
     * sortBy() transformation
     *
     * sortBy(keyfunc, ascending=true)
     *
     * Description:
     * Sorts this RDD by the given keyfunc
     * ----------------------------------------------------
     */
    val pairs = List((10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9"))
    println("pairs = " + pairs)
    val rdd = spark.sparkContext.parallelize(pairs)
    println("rdd.count(): " + rdd.count())
    println("rdd.collect(): " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     *-----------------------------------------
     ** Sort by key ascending
     *-----------------------------------------
     */
    val sortedByKeyAscending = rdd.sortByKey(ascending=true)
    println("sortedByKeyAscending.count(): " + sortedByKeyAscending.count())
    println("sortedByKeyAscending.collect(): " + sortedByKeyAscending.collect().mkString("Array(", ", ", ")"))

    /**
     *-----------------------------------------
     ** Sort by key descending
     *-----------------------------------------
     */
    val sortedByKeyDescending = rdd.sortByKey(ascending=false)
    println("sortedByKeyDescending.count(): " + sortedByKeyDescending.count())
    println("sortedByKeyDescending.collect(): " + sortedByKeyDescending.collect().mkString("Array(", ", ", ")"))

    /**
     *-----------------------------------------
     ** Sort by value ascending
     *-----------------------------------------
     */
    val sortedByValueAscending = rdd.sortBy(_._2, ascending=true)
    println("sortedByValueAscending.count(): " + sortedByValueAscending.count())
    println("sortedByValueAscending.collect(): " + sortedByValueAscending.collect().mkString("Array(", ", ", ")"))

    /**
     *-----------------------------------------
     ** Sort by value descending
     *-----------------------------------------
     */
    val sortedByValueDescending = rdd.sortBy(_._2, ascending=false)
    println("sortedByValueDescending.count(): " + sortedByValueDescending.count())
    println("sortedByValueDescending.collect(): " + sortedByValueDescending.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
