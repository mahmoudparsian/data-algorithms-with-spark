package org.data.algorithms.spark.ch10

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 *-----------------------------------------------------
 * This is a DNA-Base-Count in Spark.
 * It uses InMapper-Combiner with ReduceByKey()
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
object DNABaseCountBasicInMapperCombinerUsingReduceByKey {

  def dropCommentedRecord(fastaRecord: String): Boolean = {
    if (fastaRecord.isBlank) return false
    val stripped = fastaRecord.strip()
    if (stripped.length < 2 || stripped.startsWith(">")) false
    else
      // then it is a DNA string
      true
  }

  // create a set of (dna-letter, 1) pairs
  def immapperCombiner(fastaRecord: String): List[(String, Int)] = {
    val keyValueList = new ListBuffer[(String, Int)]
    var A = 0
    var T = 0
    var C = 0
    var G = 0
    val chars = fastaRecord.toUpperCase
    for (char <- chars)
      if (char == 'A') A += 1
      else if (char == 'T') T +=1
      else if (char == 'C') C +=1
      else if (char == 'G') G +=1
    keyValueList.addOne("A", A)
    keyValueList.addOne("T", T)
    keyValueList.addOne("C", C)
    keyValueList.addOne("G", G)
    keyValueList.toList
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
    val pairs = filtered.flatMap(immapperCombiner)
    println("pairs.count() : " + pairs.count())
    println("pairs.take(3) : " + pairs.take(3).mkString("Array(", ", ", ")"))


    val frequencies = pairs.reduceByKey((x, y) => x + y)
    println("frequencies.collect() : " + frequencies.collect().mkString("Array(", ", ", ")"))

    spark.stop()
  }

}
