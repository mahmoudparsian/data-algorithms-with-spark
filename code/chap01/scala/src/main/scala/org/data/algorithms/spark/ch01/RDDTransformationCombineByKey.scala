package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a combineByKey() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */

/**
 *-----------------------------------------
 * The goal of this exercise is to use the
 * combineByKey() transformation to find the
 * (min, max, count) per key. Given a set
 * of (key, number) pairs, find (min, max, count)
 * per key. For example, if your input is:
 * [("a", 5), ("a", 6), ("a", 7),
 *  ("b", 10), ("b", 20), ("b", 30), ("b", 40)]
 * then we will create output as:
 * [("a", (5, 7, 3)), ("b", (10, 40, 4))]
 *
 *-----------------------------------------
 */
object RDDTransformationCombineByKey {

  def createPair(t3: (String, String, Int)): (String, Int) = {
    // t3 = (name, city, number)
    val name = t3._1
    // city = t3._2
    val number = t3._3
    (name, number)
  }

  /**
   * Function<V,C> createCombiner
   * @param Integer
   * @return C as (min, max, count)
   */
  def createCombiner(V: Int) : (Int, Int, Int) = {
    (V, V, 1)
  }

  /**
   * Function2<C,V,C> mergeValue
   * @param C : Tuple3 as (min, max, count)
   * @param V : Integer
   * @return C as (min, max, count)
   */
  def mergeValue(C: (Int, Int, Int), V: Int): (Int, Int, Int) = {
    (Math.min(C._1, V), Math.max(C._2, V), C._3 + 1)
  }

  /**
   * Function2<C,C,C> mergeCombiners
   * @param C1 : Tuple3 as (min, max, count)
   * @param C2 : Tuple3 as (min, max, count)
   * @return C as (min, max, count)
   */
  def mergeCombiners(C1: (Int, Int, Int), C2: (Int, Int, Int)): (Int, Int, Int) = {
    (Math.min(C1._1, C2._1), Math.max(C1._2, C2._2), C1._3 + C2._3)
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * combineByKey() transformation
     *
     * source_rdd.combineByKey(lambda1, lambda2, lambda3) --> target_rdd
     *
     * RDD<K,C> combineByKey(
     *                       Function<V,C> createCombiner,
     *                       Function2<C,V,C> mergeValue,
     *                       Function2<C,C,C> mergeCombiners
     *                      )
     *
     * Simplified version of combineByKey that hash-partitions the
     * resulting RDD using the existing partitioner/parallelism level
     * and using map-side aggregation.
     *
     * ----------------------------------------------------
     *
     * Create a list of tuples.
     * Each tuple contains name, city, and age.
     * Create a RDD from the list above.
     */
    val listOfTuples=
      List(("alex","Sunnyvale", 25),
        ("alex","Sunnyvale", 33),
        ("alex","Sunnyvale", 45),
        ("alex","Sunnyvale", 63),
        ("mary", "Ames", 22),
        ("mary", "Cupertino", 66),
        ("mary", "Ames", 20),
        ("bob", "Ames", 26))
    println("listOfTuples = " + listOfTuples)
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     *  apply a map() transformation to rdd
     *  create a (key, value) pair
     *   where
     *        key is the name (first element of tuple)
     *        value is a number
     * ----------------------------------------------
     */
    val rdd2 = rdd.map(createPair)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     * Find (min, max, count) per key:
     *
     * apply a combineByKey() transformation to rdd2
     * create a (key, value) pair
     *  where
     *       key is the city name
     *       value is (min, max, count)
     * ----------------------------------------------
     */
    val combiner = rdd2.combineByKey(createCombiner, mergeValue, mergeCombiners)
    println("rdd3 = " + combiner)
    println("rdd3.count() = " + combiner.count())
    println("rdd3.collect() = " + combiner.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
