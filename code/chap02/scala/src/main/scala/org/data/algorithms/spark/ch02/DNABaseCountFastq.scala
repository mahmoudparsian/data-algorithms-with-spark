package org.data.algorithms.spark.ch02

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 *-----------------------------------------------------
 * Version-3
 * This is a DNA-Base-Count in Spark using FASQT input data.
 * The goal is to show how "DNA-Base-Count" works.
 *------------------------------------------------------
 * Input Parameters:
 *    argv[1]: String, input path for FASTQ data
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */

object DNABaseCountFastq {

  def drop3Records(rec: String) : Boolean = {
    val firstChar = rec.charAt(0)
    // drop Line 1
    if (firstChar == '@')
      return false
    // drop Line 3
    if (firstChar == '+')
      return false

    // drop Line 4 & keep Line 2
    val nonDnaLetters = Set.from("-+*/<>=.:@?;0123456789bde".toCharArray)
    if(rec.toCharArray.exists(nonDnaLetters.contains)){
      // drop Line 4
      println("rec=", rec)
      false
    } else
      // found Line 2: DNA Sequence
      true
  }

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

  /**
   *-----------------------------------
   * rec: a single record of FASTQ file
   *
   * drop Lines 1, 3, and 4
   * keep only Line2, which is a DNA sequence
   *-----------------------------------
   */

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Input path is missing in the command line argument")
      System.exit(-1)
    }

    val spark =
      SparkSession.builder()
        .appName("DNABaseCountFastq")
        .master("local[*]")
        .getOrCreate()

    val inputPath = args(0)
    println(s"inputPath : $inputPath")

    val recordsRdd =
      spark.sparkContext.textFile(inputPath)
        .map(_.toLowerCase)
    println(s"recordsRdd : ${recordsRdd.collect().mkString("Array(", ", ", ")")}")

    // for every seq (as 4 lines), drop 3 lines
    val dnaSeqs = recordsRdd.filter(drop3Records)
    println("dnaSeqs : ", dnaSeqs.collect().mkString("Array(", ", ", ")"))

    // if you do not have enough RAM, then do the following
    // recordsRdd.persist(StorageLevel.MEMORY_AND_DISK)

    val pairsRdd = dnaSeqs.mapPartitions(processFASTQPartition)
    println("pairsRdd : debug")
    val pairsAsList = pairsRdd.collect()
    println("pairsAsList : ", pairsAsList.mkString("Array(", ", ", ")"))

    val frequenciesRdd = pairsRdd.reduceByKey((x, y) => x + y)
    println("frequenciesRdd : debug")
    val frequenciesAsList = frequenciesRdd.collect()
    println("frequenciesAsList : ", frequenciesAsList.mkString("Array(", ", ", ")"))

    spark.stop()


  }

}
