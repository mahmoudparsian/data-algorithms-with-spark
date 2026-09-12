package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a takeOrdered() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDActionTakeOrdered {

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // create a SparkContext object
    val sc = spark.sparkContext
    println("sc = " +  sc)

    /**
     * ----------------------------------------------------
     * takeOrdered() transformation
     *
     * takeOrdered(N, key=None)
     * Get the N elements from an RDD ordered
     * in ascending order or as specified by
     * the optional key function.
     *
     * ----------------------------------------------------
     */
    val numbers = List(8, 10, 1, 2, 9, 3, 4, 5, 6, 7)
    println("numbers = " + numbers)

    val top3 = sc.parallelize(numbers).takeOrdered(3)
    println("top3 = " + top3.mkString("Array(", ", ", ")"))

    val bottom3 = sc.parallelize(numbers).takeOrdered(3)(Ordering.Int.reverse)
    println("bottom3 = " + bottom3.mkString("Array(", ", ", ")"))

    val pairs = List((10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9"))
    println("pairs = " + pairs)

    val top3Pairs = sc.parallelize(pairs).takeOrdered(3)
    println("top3Pairs = " + top3Pairs.mkString("Array(", ", ", ")"))

    val bottom3Pairs = sc.parallelize(pairs).takeOrdered(3)(Ordering.Int.reverse.on(_._1))
    println("bottom3Pairs = " + bottom3Pairs.mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
