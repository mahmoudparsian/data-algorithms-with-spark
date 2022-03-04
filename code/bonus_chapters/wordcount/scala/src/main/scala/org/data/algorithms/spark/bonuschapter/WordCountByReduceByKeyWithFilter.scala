package org.data.algorithms.spark.bonuschapter

import org.apache.spark.sql.SparkSession

/**
 *----------------------------------------
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Biman Mandal
 *----------------------------------------
 */
object WordCountByReduceByKeyWithFilter {
  def main(args: Array[String]): Unit = {
    // create an instance of a SparkSession as spark
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    // set input path
    val inputPath = args(0)
    println("inputPath=" + inputPath)

    // drop words if its length are less than WORD_LENGTH_THRESHOLD
    val WORD_LENGTH_THRESHOLD = args(1).toInt
    println("WORD_LENGTH_THRESHOLD=" + WORD_LENGTH_THRESHOLD)

    // drop words (after reduction) if its frequency is less than FREQUENCY_THRESHOLD
    val FREQUENCY_THRESHOLD = args(2).toInt
    print("FREQUENCY_THRESHOLD=" + FREQUENCY_THRESHOLD)

    // create RDD from a text file
    val records = spark.sparkContext.textFile(inputPath)
    println(records.collect().mkString("Array(", ", ", ")"))

    val words = records.flatMap(line => line.split(" "))
    println(words.collect().mkString("Array(", ", ", ")"))

    // map side filter: filter():
    // drop words if its length are less than WORD_LENGTH_THRESHOLD
    val wordsFiltered = words.filter(word => word.length >= WORD_LENGTH_THRESHOLD)
    println("wordsFiltered=" + wordsFiltered.collect().mkString("Array(", ", ", ")"))

    val pairs =  wordsFiltered.map(word => (word, 1))
    print(pairs.collect().mkString("Array(", ", ", ")"))

    val frequencies = pairs.reduceByKey((x, y) => x + y)
    println(frequencies.collect().mkString("Array(", ", ", ")"))

    // reducer side filter: filter():
    // drop words (after reduction) if its frequency is less than FREQUENCY_THRESHOLD
    val filteredFrequencies = frequencies.filter(x => x._2 >= FREQUENCY_THRESHOLD)
    println("filteredFrequencies=" + filteredFrequencies.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }

}
