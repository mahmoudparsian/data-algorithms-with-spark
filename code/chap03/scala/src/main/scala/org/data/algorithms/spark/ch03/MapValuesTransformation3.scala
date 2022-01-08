package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

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
object MapValuesTransformation3 {

  /**
   * Count DNA bases
   */
  def countDNABases(dnaSeq: String): Map[Char, Int] = {
    val counter = new mutable.HashMap[Char, Int]()
    for (letter <- dnaSeq)
      counter.put(letter, counter.getOrElse(letter, 0) + 1)
    counter.toMap
  }


  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark =
      SparkSession.builder()
        .appName("MapValuesTransformation3")
        .master("local[*]")
        .getOrCreate()

    // Create a list of arrays.
    val listOfKeyValue= List( ("seq1", "AATCGGCCAAAGG"), ("seq2", "ATTATCGGCCAAAGCTTTCG"), ("seq1", "AAATTATCGGCCAAAGG"))
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
     * DNA Base Count: find count of A's, T's, C's, G's
     * rddMapped : RDD[(String, dictionary)]
     *  where dictionary is a hash table with keys {A, T, C, G}
     *
     */
    val dnaBaseCount = rdd.mapValues(countDNABases)
    println("dnaBaseCount = " + dnaBaseCount)
    println("dnaBaseCount.count() = " + dnaBaseCount.count())
    println("dnaBaseCount.collect() = " + dnaBaseCount.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

