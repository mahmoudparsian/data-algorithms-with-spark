package org.data.algorithms.spark.ch10

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 *--------------------------------------------
 * This
 * program is to count frequencies of unique
 * frequencies of DNA code: by using the
 * basic map() and reduce() transformations.
 */
object InMapperCombinerUsingMapReduce {
  /**
   *--------------------------------------------
   * First we define a simple function, which accepts
   * a single input partition (comprised of many input
   * records) and then returns a list of (key, value)
   * pairs, where key is a character and value 1.
   */
  def createPairs(record: String): List[(String, Int)] = {
    val words = record.toUpperCase().split(" ")
    words.flatMap(word => word.toCharArray).map(c => (c.toString ,1)).toList
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    println("inputPath : " + inputPath)

    // create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // records: RDD[String]
    val records = spark.sparkContext.textFile(inputPath)
    println("records.count() : " + records.count())
    println("records.collect(): " + records.collect().mkString("Array(", ", ", ")"))
    println("records.getNumPartitions(): " + records.getNumPartitions)

    /**
     * The flatMap() transformation returns a new
     * RDD[key, value] (where key is a DNA code and
     * value is 1) by applying a function to each
     * element of the records RDD[String]
     */
    val pairs = records.flatMap(createPairs)
    println("pairs.count() : " + pairs.count())
    println("pairs.collect(): " + pairs.collect().mkString("Array(", ", ", ")"))

    val frequencies = pairs.reduceByKey((x, y) => x + y)
    println("frequencies.count(): " + frequencies.count())
    println("frequencies.collect(): " + frequencies.collect().mkString("Array(", ", ", ")"))

    spark.stop()
  }

}

/*
records.count() : 11
records.collect(): Array(ATCGGGATCCGGG, ATTCCGGGATTCCCC, ATGGCCCCCGGGATCGGG, CGGTATCCGGGGAAAAA, aaattCCGGAACCGGGGGTTT, CCTTTTATCGGGCAAATTTTCCCGG, attttcccccggaaaAAATTTCCGGG, ACTGACTAGCTAGCTAACTG, GCATCGTAGCTAGCTACGAT, AATTCCCGCATCGATCGTACGTACGTAG, ATCGATCGATCGTACGATCG)
records.getNumPartitions(): 2
pairs.count() : 223
pairs.collect(): Array((A,1), (T,1), (C,1), (G,1), (G,1), (G,1), (A,1), (T,1), (C,1), (C,1), (G,1), (G,1), (G,1), (A,1), (T,1), (T,1), (C,1), (C,1), (G,1), (G,1), (G,1), (A,1), (T,1), (T,1), (C,1), (C,1), (C,1), (C,1), (A,1), (T,1), (G,1), (G,1), (C,1), (C,1), (C,1), (C,1), (C,1), (G,1), (G,1), (G,1), (A,1), (T,1), (C,1), (G,1), (G,1), (G,1), (C,1), (G,1), (G,1), (T,1), (A,1), (T,1), (C,1), (C,1), (G,1), (G,1), (G,1), (G,1), (A,1), (A,1), (A,1), (A,1), (A,1), (A,1), (A,1), (A,1), (T,1), (T,1), (C,1), (C,1), (G,1), (G,1), (A,1), (A,1), (C,1), (C,1), (G,1), (G,1), (G,1), (G,1), (G,1), (T,1), (T,1), (T,1), (C,1), (C,1), (T,1), (T,1), (T,1), (T,1), (A,1), (T,1), (C,1), (G,1), (G,1), (G,1), (C,1), (A,1), (A,1), (A,1), (T,1), (T,1), (T,1), (T,1), (C,1), (C,1), (C,1), (G,1), (G,1), (A,1), (T,1), (T,1), (T,1), (T,1), (C,1), (C,1), (C,1), (C,1), (C,1), (G,1), (G,1), (A,1), (A,1), (A,1), (A,1), (A,1), (A,1), (T,1), (T,1), (T,1), (C,1), (C,1), (G,1), (G,1), (G,1), (A,1), (C,1), (T,1), (G,1), (A,1), (C,1), (T,1), (A,1), (G,1), (C,1), (T,1), (A,1), (G,1), (C,1), (T,1), (A,1), (A,1), (C,1), (T,1), (G,1), (G,1), (C,1), (A,1), (T,1), (C,1), (G,1), (T,1), (A,1), (G,1), (C,1), (T,1), (A,1), (G,1), (C,1), (T,1), (A,1), (C,1), (G,1), (A,1), (T,1), (A,1), (A,1), (T,1), (T,1), (C,1), (C,1), (C,1), (G,1), (C,1), (A,1), (T,1), (C,1), (G,1), (A,1), (T,1), (C,1), (G,1), (T,1), (A,1), (C,1), (G,1), (T,1), (A,1), (C,1), (G,1), (T,1), (A,1), (G,1), (A,1), (T,1), (C,1), (G,1), (A,1), (T,1), (C,1), (G,1), (A,1), (T,1), (C,1), (G,1), (T,1), (A,1), (C,1), (G,1), (A,1), (T,1), (C,1), (G,1))
frequencies.count(): 4
frequencies.collect(): Array((T,53), (G,60), (A,51), (C,59))
 */
