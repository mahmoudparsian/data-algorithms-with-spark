package org.data.algorithms.spark.ch10

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *-----------------------------------------------------
 * This is a DNA-Base-Count in Spark.
 * It uses mapPartitions() transformation
 *------------------------------------------------------
 * NOTE: print() and collect() are used for
 *       debugging and educational purposes.
 *------------------------------------------------------
 * Input Parameters:
 *    args(0): input path
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DNABaseCountBasicUsingMappartitions {

  def dropCommentedRecord(fastaRecord: String): Boolean = {
    if (fastaRecord.isBlank) return false
    val stripped = fastaRecord.strip()
    if (stripped.length < 2 || stripped.startsWith(">")) false
    else
      // then it is a DNA string
      true
  }

  // create a set of (dna-letter, 1) pairs
  def processFastaPerPartition(partition: Iterator[String]): Iterator[(String, Int)] = {
    val hashMap = new mutable.HashMap[String, Int]()
    for (element <- partition) {
      val chars = element.toUpperCase
      for (char <- chars)
        if (Set('A', 'T', 'C', 'G').contains(char))
          hashMap.put(char.toString, hashMap.getOrElse(char.toString, 0) + 1)
    }
    hashMap.iterator
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    println("inputPath : " + inputPath)

    // create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // records: RDD[String]
    val records = spark.sparkContext.textFile(inputPath)
    println("records.count() : " + records.count())

    // drop non-needed records
    // filtered: RDD[String]
    val filtered = records.filter(dropCommentedRecord)
    println("filtered.count() : " + filtered.count())

    // pairs: RDD[(String, Integer)]
    val countsPerPartition = filtered.mapPartitions(processFastaPerPartition)
    println("countsPerPartition.count() : " + countsPerPartition.count())
    println("countsPerPartition.collect() : " + countsPerPartition.collect().mkString("Array(", ", ", ")"))

    val frequencies = countsPerPartition.reduceByKey((x, y) => x + y)
    println("frequencies.collect() : " + frequencies.collect().mkString("Array(", ", ", ")"))

    spark.stop()
  }

}
