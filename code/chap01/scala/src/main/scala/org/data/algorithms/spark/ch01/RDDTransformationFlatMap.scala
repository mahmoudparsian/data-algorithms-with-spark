package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 *-------------------------------------------------------
 * Apply a flatMap() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDTransformationFlatMap {

  def tokenize(record: String): List[String] = {
    val tokens = record.split(" ")
    val myList = new ListBuffer[String]()
    tokens.foreach(word => {
      if (word.length > 2) myList += word
    })
    myList.toList
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * flatMap() transformation
     *
     * source_rdd.flatMap(function) --> target_rdd
     *
     * flatMap() is a 1-to-M transformation
     *
     * flatMap(f, preservesPartitioning=False)[source]
     * Return a new RDD by first applying a function
     * to all elements of this RDD, and then flattening 
     * the results.
     *
     * ----------------------------------------------------
     *
     * Create a list of String.
     * Each string contains a set of words.
     * Create a RDD from the list above.
     */
    val listOfStrings=
      List("of", "a fox jumped", "fox jumped of fence", "a foxy fox jumped high")
    println("listOfStrings = " + listOfStrings)
    val rdd = spark.sparkContext.parallelize(listOfStrings)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     * 1. apply a flatMap() transformation to rdd
     * 2. tokenize each string and then flatten words
     * 3. Ignore words if length is less than 3
     * ----------------------------------------------
     */
    val rdd2 = rdd.flatMap(tokenize)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
