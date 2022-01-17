package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a DataFrame from a JSON file
 * Input: JSON File
 * In this example, there is one JSON object per line.
 *------------------------------------------------------
 * Input Parameters:
 *    a JSON file
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceJSONReaderSingleLine {

  def debugFile(fileName: String): Unit = {
    val bufferedSource = scala.io.Source.fromFile(fileName)
    println(bufferedSource.mkString)
    bufferedSource.close()
  }

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
     * Spark enable us to read JSON files
     * and create a new DataFrame
     *
     * The following example reads a JSON file
     * and creates a new DataFrame:
     */
    val df = spark.read
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
