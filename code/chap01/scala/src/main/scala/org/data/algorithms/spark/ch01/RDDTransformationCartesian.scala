package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a cartesian() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDTransformationCartesian {

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * cartesian() transformation
     *
     * cartesian(other)
     * Return the Cartesian product of this RDD
     * and another one, that is, the RDD of all
     * pairs of elements (a, b) where a is in
     * self (as source-one RDD) and b is in other
     * (as source-two RDD).
     *
     * ----------------------------------------------------
     */
    val a = List(("a", 2), ("b", 3), ("c", 4))
    println("a = " + a)

    val b = List(("p", 50), ("x", 60), ("y", 70), ("z", 80) )
    println("b = " + b)

    val rdd = spark.sparkContext.parallelize(a)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    val rdd2 = spark.sparkContext.parallelize(b)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    val cart = rdd.cartesian(rdd2)
    println("cart = " + cart)
    println("cart.count() = " + cart.count())
    println("cart.collect() = " + cart.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
