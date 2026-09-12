package org.data.algorithms.spark.bonuschapter

import org.apache.spark.sql.SparkSession

/**
 *----------------------------------------
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Biman Mandal
 *----------------------------------------
 */
object WordCountByGroupByKeyShorthand {
  def main(args: Array[String]): Unit = {
    // create an instance of a SparkSession as spark
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    // set input path
    val inputPath = args(0)
    println("inputPath=" + inputPath)

    val frequencies = spark.sparkContext.textFile(inputPath)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupByKey()
      .map(x => (x._1, x._2.toList.sum))

    println(frequencies.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }

}
