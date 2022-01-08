package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * map() is a 1-to-1 transformation
 * Apply a map() transformation to an RDD
 * Input: NONE
 *
 * print() is used for educational purposes.
 *------------------------------------------------------
 * Input Parameters:
 *    file
 *-------------------------------------------------------
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object MapTransformation1FromFile {

  def createPair(t: String): (String, Int) = {
    val tokens = t.split(",")
    // t3 = (name, city, age)
    val name = tokens(0)
    val age = tokens(2).toInt
    (name, age)
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * map() transformation
     *
     * source_rdd.map(function) --> target_rdd
     *
     * map() is a 1-to-1 transformation
     *
     * map(f, preservesPartitioning=False)[source]
     * Return a new RDD by applying a function to each
     * element of this RDD.
     *
     * ----------------------------------------------------
     *
     * Create a RDD from csv file.
     */

    // define input path
    val inputPath = args(0)
    println("inputPath = " + inputPath)

    // create rdd : RDD[String]
    val rdd = spark.sparkContext.textFile(inputPath)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     *  apply a map() transformation to rdd
     *  create a (key, value) pair
     *   where
     *        key is the name (first element of tuple)
     *        value is the last element of tuple
     * ----------------------------------------------
     * rdd2: RDD[(name, age)]
     */
    val rdd2 = rdd.map(v => createPair(v))
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * increment age by 5
     *------------------------------------
     * rdd3 : RDD[(name, age)], where age is incremented by 5
     */
    val rdd3 = rdd2.mapValues(_ + 5)
    println("rdd3 = " + rdd3)
    println("rdd3.count() = " + rdd3.count())
    println("rdd3.collect() = " + rdd3.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

