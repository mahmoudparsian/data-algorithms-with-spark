package org.data.algorithms.spark.ch02

import org.apache.spark.sql.SparkSession
import org.data.algorithms.spark.ch02.DNABaseCountFastq.{drop3Records, processFASTQPartition}

import scala.collection.mutable

/**
 *-----------------------------------------------------
 * Version-3
 * This is a DNA-Base-Count in Spark Using Scala.
 * The goal is to show how "DNA-Base-Count" works.
 *------------------------------------------------------
 * Input Parameters:
 *    String, input path
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DNABaseCountVER3 {

  /**
   *-----------------------------------
   *
   * we get an iterator: which represents
   * a single partition of source RDD
   *
   * This function creates a hash map of DNA Letters
   * "z" denotes the total number of FASTA Records for a single partition
   *
   */
  def processFASTQPartition(iterator: Iterator[String]): Iterator[(Char, Int)] = {
    // create an empty dictionary
    val hashMap = new mutable.HashMap[Char, Int]()

    iterator.foreach(fastqRecord => {
      hashMap.put('z', hashMap.getOrElse('z', 0) + 1)
      val chars = fastqRecord.toLowerCase
      for (c <- chars)
        hashMap.put(c, hashMap.getOrElse(c, 0) + 1)
    })

    hashMap.iterator
  }

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DNABaseCountVER3")
        .master("local[*]")
        .getOrCreate()

    val inputPath = args(0)
    println(s"inputPath : $inputPath")

    val recordsRdd =
      spark.sparkContext.textFile(inputPath)
        .map(_.toLowerCase)
    println(s"recordsRdd : ${recordsRdd.collect().mkString("Array(", ", ", ")")}")

    // if you do not have enough RAM, then do the following
    // recordsRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val pairsRdd = recordsRdd.mapPartitions(processFASTQPartition)
    println("pairsRdd : debug")

    val frequenciesRdd = pairsRdd.reduceByKey((x, y) => x + y)
    println("frequenciesRdd : debug")
    val frequenciesAsList = frequenciesRdd.collect()
    println("frequenciesAsList : ", frequenciesAsList.mkString("Array(", ", ", ")"))

    spark.stop()
  }

}
