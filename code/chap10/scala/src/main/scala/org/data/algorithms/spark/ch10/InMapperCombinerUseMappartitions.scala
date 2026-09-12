package org.data.algorithms.spark.ch10

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 *--------------------------
 * The
 * program is to count frequencies of unique
 * frequencies of DNA code: by using the
 * mapPartitions() transformation
 *--------------------------
 */

object InMapperCombinerUseMappartitions {
  /**
   *--------------------------------------------
   * First we define a simple function, which accepts
   * a single input partition (comprised of many input
   * records) and then returns a list of (key, value)
   * pairs, where key is a character and value is an
   * aggregated frequency of that character.
   */
  def immapperCombiner(partitionIterator: Iterator[String]): Iterator[(String, Int)] = {
    println("partitionIterator= " + partitionIterator)
    val hashMap = new mutable.HashMap[String, Int]()
    for (record <- partitionIterator) {
      val words = record.toUpperCase.split(" ")
      for (word <- words) {
        for (char <- word)
          hashMap.put(char.toString, hashMap.getOrElse(char.toString, 0) + 1)
      }
    }
    println(s"hashMap = $hashMap")
    val pairs = hashMap.iterator
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
    val pairs = records.mapPartitions(immapperCombiner)
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
partitionIterator= <iterator>
partitionIterator= <iterator>
hashMap = HashMap(A -> 28, C -> 36, T -> 31, G -> 40)
hashMap = HashMap(A -> 23, C -> 23, T -> 22, G -> 20)
pairs.count() : 8
partitionIterator= <iterator>
hashMap = HashMap(A -> 23, C -> 23, T -> 22, G -> 20)
partitionIterator= <iterator>
hashMap = HashMap(A -> 28, C -> 36, T -> 31, G -> 40)
pairs.collect(): Array((A,28), (C,36), (T,31), (G,40), (A,23), (C,23), (T,22), (G,20))
partitionIterator= <iterator>
partitionIterator= <iterator>
hashMap = HashMap(A -> 23, C -> 23, T -> 22, G -> 20)
hashMap = HashMap(A -> 28, C -> 36, T -> 31, G -> 40)
frequencies.count(): 4
frequencies.collect(): Array((T,53), (G,60), (A,51), (C,59))
 */
