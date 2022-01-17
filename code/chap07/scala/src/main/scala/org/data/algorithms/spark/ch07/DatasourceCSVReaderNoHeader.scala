package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a DataFrame from a CSV file Without a Header
 * Input: CSV File Without a Header
 *------------------------------------------------------
 * Input Parameters:
 *    a CSV file Without a Header
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceCSVReaderNoHeader {

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
     * Spark enable us to read CSV files with or without a header.
     * Here we will read a CSV file without a header and create
     * a new DataFrame
     *
     * The following example reads a CSV file without a
     * header and create a new DataFrame and infers a
     * schema from the content of columns:
     */
    val df = spark
      .read
      .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
      .option("header","false")
      .option("inferSchema", "true")
      .load(inputPath)
    println("df = " + df.collect().mkString("Array(", ", ", ")"))
    df.show()
    df.printSchema()


    // You may rename column names of a DataFrame
    // change default column names to your desired column names
    val df2 = df.selectExpr("_c0 as name", "_c1 as city", "_c2 as age")
    df2.show()
    df2.printSchema()

    // done!
    spark.stop()
  }

}
