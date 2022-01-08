package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * flatMap() is a 1-to-Many transformation:
 * each source element can be mapped to zero,
 * 1, 2, 3, ... , or more elements
 *
 * Apply a flatMap() transformation to an RDD
 * Input: NONE
 *
 * print() is used for educational purposes.
 *------------------------------------------------------
 * Input Parameters:
 *    file
 *-------------------------------------------------------
 * @author Biman Mandal
 *-------------------------------------------------------
 * Find Frequency of Bigrams:
 *
 * In this example, we read a file and find
 * frequency of bigrams. If a record is empty
 * or has a single word, then that record will
 * be dropped.
 *

 */
object FlatMapTransformation1FromFile {

  /**
   *=========================================
   * rec = an element of source RDD = "word1 word2 word3 word4"
   * create bigrams as a list [(word1, word2), (word2, word3), (word3, word4)]
   *
   */
  def createBigrams(rec: String): List[(String, String)] = {
    val recStripped = rec.trim
    if (recStripped.length < 1) return List.empty
    val tokens = recStripped.split(" ")
    if (tokens.length < 2) return List.empty
    tokens.sliding(2).toList.map(ele => (ele(0), ele(1)))
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     *----------------------------------------
     * flatMap() transformation
     *
     * source_rdd.flatMap(function) --> target_rdd
     *
     * flatMap() is a 1-to-Many transformation
     *
     * Spark's flatMap() is a transformation operation that flattens
     * the RDD/DataFrame (array/map DataFrame columns) after applying the
     * function on every element and returns a new Spark RDD/DataFrame.
     *
     *----------------------------------------
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
     *------------------------------------
     * apply a flatMap() transformation to rdd
     *------------------------------------
     * bigrams : RDD[[(word1, word2)]]
     */
    val bigrams = rdd.flatMap(createBigrams)
    println("bigrams = " + bigrams)
    println("bigrams.count() = " + bigrams.count())
    println("bigrams.collect() = " + bigrams.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * find frequency of bigrams
     *------------------------------------
     * keyValue : create (bigram, 1) pairs
     */
    val keyValue = bigrams.map((_, 1))
    println("keyValue = " + keyValue)
    println("keyValue.count() = " + keyValue.count())
    println("keyValue.collect() = " + keyValue.collect().mkString("Array(", ", ", ")"))

    val frequency = keyValue.reduceByKey(_ + _)
    println("frequency = " + frequency)
    println("frequency.count() = " + frequency.count())
    println("frequency.collect() = " + frequency.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

