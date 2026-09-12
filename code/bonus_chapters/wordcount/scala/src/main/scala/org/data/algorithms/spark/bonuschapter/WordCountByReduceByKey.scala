package org.data.algorithms.spark.bonuschapter

import org.apache.spark.sql.SparkSession

/**
 *----------------------------------------
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Biman Mandal
 *----------------------------------------
 */
object WordCountByReduceByKey {
  def main(args: Array[String]): Unit = {
    // create an instance of a SparkSession as spark
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    // set input path
    val inputPath = args(0)
    println("inputPath=" + inputPath)

    // create RDD from a text file
    val records = spark.sparkContext.textFile(inputPath)
    println(records.collect().mkString("Array(", ", ", ")"))

    val words = records.flatMap(line => line.split(" "))
    println(words.collect().mkString("Array(", ", ", ")"))

    val pairs =  words.map(word => (word, 1))
    println(pairs.collect().mkString("Array(", ", ", ")"))

    val frequencies = pairs.reduceByKey((x, y) => x + y)
    println(frequencies.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }

}
