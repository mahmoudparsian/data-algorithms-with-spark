package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

import java.io.File

/**
 *-----------------------------------------------------
 * Write Content of a DataFrame to a CSV Path
 * Input: Output CSV File Path
 *------------------------------------------------------
 * Input Parameters:
 *    Output CSV File Path
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceCSVWriter {

  def dumpDirectory(dir: String): Unit = {
    println("output dir name: ", dir)
    //contents of the current directory
    val dirListing = new File(dir)
    println("dirListing: " + dirListing.listFiles().map(_.getName).mkString(","))
    dirListing
      .listFiles
      .filter(_.isFile)
      .filter(_.getName.startsWith("part"))
      .foreach(file =>{
        println("output file name: " +  file.getPath)
        debugFile(file.getPath)
      })
  }

  def debugFile(fileName: String) = {
    val bufferedSource = scala.io.Source.fromFile(fileName)
    println(bufferedSource.mkString)
    bufferedSource.close()
  }

  def main(args: Array[String]): Unit = {
    // read name of Output CSV File Name
    val outputCsvFilePath = args(0)
    println("outputCsvFilePath : "+ outputCsvFilePath)

    // create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    /**
     * Write to a CSV File
     * Use the SparkSession.createDataFrame() function to create a DataFrame.
     * In the following example, createDataFrame() takes a list of tuples containing
     * names, cities, and ages, and a list of column names:
     */
    val columnNames = List("name", "city", "age")
    val people = spark.createDataFrame(List(
      ("Alex", "Ames", 50),
      ("Alex", "Sunnyvale", 51),
      ("Alex", "Stanford", 52),
      ("Gandalf", "Cupertino", 60),
      ("Thorin", "Sunnyvale", 95),
      ("Max", "Ames", 55),
      ("George", "Cupertino", 60),
      ("Terry", "Sunnyvale", 95),
      ("Betty", "Ames", 78),
      ("Brian", "Stanford", 77))).toDF(columnNames: _*)

    // Write the people DataFrame to a CSV File
    people
      .write
      .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
      .save(outputCsvFilePath)

    //check to see the content of output path
    dumpDirectory(outputCsvFilePath)


    // To display the contents of the DataFrame, use the show() method.
    people.show(10, truncate=false)

    println("people.count() = " + people.count())

    println("people.collect() = " + people.collect().mkString("Array(", ", ", ")"))

    // Spark samples the records to infer the schema of the collection.
    people.printSchema()

    // done!
    spark.stop()
  }

}
