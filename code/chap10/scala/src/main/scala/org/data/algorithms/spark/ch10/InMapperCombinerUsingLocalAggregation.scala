package org.data.algorithms.spark.ch10

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *--------------------------
 * The
 * program is to count frequencies of unique
 * frequencies of DNA code: by using the
 * local aggregation per record level: for
 * each record, we return a dictionary[char, frequency].
 * Then aggregate them for total frequency
 * per unique character.
 *--------------------------

 *<1> record represents a single input record
 *<2> Create an empty dictionary[String, Integer]
 *<3> Tokenize record into an array of words
 *<4> Iterate over words
 *<5> Iterate over each word
 *<6> Aggregate characters
 *<7> Flatten dictionary into a list of (character, frequency)
 *<8> return the flattened list of (character, frequency)
 */

object InMapperCombinerUsingLocalAggregation {
  /**
   *--------------------------------------------
   * First we define a simple function, which accepts
   * a single input record and then returns a list of
   * (key, value) pairs, where key is a character and
   * value is an aggregated frequency of that character
   * (for the given record)
   */
  // <1>
  def immapperCombiner(record: String): Iterator[(String, Int)] = {
    // <2>
    // println("record=", record)
    val hashMap = new mutable.HashMap[String, Int]()
    // <3>
    val words = record.toUpperCase.split(" ")
    // <4>
    for (word <- words) {
      // <5>
      for (char <- word)
        // <6>
        hashMap.put(char.toString, hashMap.getOrElse(char.toString, 0) + 1)
    }
    // <7>
    val pairs = hashMap.iterator

    // <8>
    pairs
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
     * RDD by applying a function to each elememt of
     * this RDD
     */
    val pairs = records.flatMap(immapperCombiner)
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
pairs.count() : 44
pairs.collect(): Array((A,2), (C,3), (T,2), (G,6), (A,2), (C,6), (T,4), (G,3), (A,2), (C,6), (T,2), (G,8), (A,6), (C,3), (T,2), (G,6), (A,5), (C,4), (T,5), (G,7), (A,4), (C,7), (T,9), (G,5), (A,7), (C,7), (T,7), (G,5), (A,6), (C,5), (T,5), (G,4), (A,5), (C,5), (T,5), (G,5), (A,7), (C,8), (T,7), (G,6), (A,5), (C,5), (T,5), (G,5))
frequencies.count(): 4
frequencies.collect(): Array((T,53), (G,60), (A,51), (C,59))
 */
