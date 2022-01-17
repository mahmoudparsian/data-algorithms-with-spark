package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a DataFrame from a CSV file With Header
 * Input: CSV File With Header
 *------------------------------------------------------
 * Input Parameters:
 *    a CSV file With Header
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceCSVReaderHeader {

  def debugFile(fileName: String) = {
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
     * Create a DataFrame from a given input file
     *=====================================
     *
     * Spark enable us to read CSV files with a header.
     * Basically, a header is a CSV string of column names.
     *
     * The following example reads a CSV file with a
     * header and create a new DataFrame and infers a
     * schema from the content of columns:
     */
    val df = spark
      .read
      .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
      .option("header","true")
      .option("inferSchema", "true")
      .load(inputPath)
    println("df.count() = " + df.count())
    println("df = " + df.collect().mkString("Array(", ", ", ")"))
    df.show()
    df.printSchema()

    // done!
    spark.stop()
  }

}
