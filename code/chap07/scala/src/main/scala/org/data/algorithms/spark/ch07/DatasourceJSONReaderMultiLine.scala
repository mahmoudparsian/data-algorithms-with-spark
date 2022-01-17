package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a DataFrame from a JSON file
 * Input: JSON File
 * In this example, JSON object occupies multiple lines.
 * Then we must enable multi-line mode for Spark to load
 * the JSON file. Files will be loaded as a whole entity
 * and cannot be split.
 *------------------------------------------------------
 * Input Parameters:
 *    a JSON file
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceJSONReaderMultiLine {

  def debugFile(fileName: String): Unit =
    println(scala.io.Source.fromFile(fileName).mkString)

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    // read name of input file
    val inputPath = args(0)
    println("input path : " + inputPath)
    debugFile(inputPath)

    /**
     *=====================================
     * Create a DataFrame from a given input JSON file
     *=====================================
     *
     * Spark enable us to read multi-line JSON files
     * and create a new DataFrame
     *
     * The following example reads a multi-line JSON file
     * and creates a new DataFrame:
     */
    val df = spark.read
      .option("multiline", "true")
      .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
      .load(inputPath)

    println("df.count() = " + df.count())

    println("df = " + df.collect().mkString("Array(", ", ", ")"))

    df.show(10, truncate = false)

    df.printSchema()

    // done!
    spark.stop()
  }

}
