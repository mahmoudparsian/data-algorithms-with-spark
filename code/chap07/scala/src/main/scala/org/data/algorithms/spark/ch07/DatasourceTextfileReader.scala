package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

import scala.io.Source
/**
 *-----------------------------------------------------
 * Read Content of file to RDD
 * Input: File Path
 *------------------------------------------------------
 * Input Parameters:
 *    Path
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object DatasourceTextfileReader {

  def debugFile(inputPath: String) = {
    val bufferedSource = Source.fromFile(inputPath)
    println(bufferedSource.mkString)
    bufferedSource.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: DatasourceTextfileReader <output-path>")
      System.exit(-1)
    }
    //create an instance of SparkSession
    val spark =
      SparkSession.
        builder().
        master("local[*]").
        getOrCreate()

    // read name of input file
    val inputPath = args(0)
    println(s"inputPath: ${inputPath}")
    debugFile(inputPath)
    /*
    ================================================
    # Create an RDD[String] from a given Text File
    ================================================
    */
    val records = spark.sparkContext.textFile(inputPath)
    println(s"records = ${records}")
    println(s"records.count() = ${records.count()}")
    println(s"records.collect() = ${records.collect().mkString("[",",","]")}")
    /*
    #================================================
    # Transform an RDD[String] to RDD[Integer]
    #================================================
    */
    val numbers = records.flatMap(rec =>  rec.split(",")).map(_.toInt)
    println(s"numbers = ${numbers}")
    println(s"numbers.count() = ${numbers.count()}")
    println(s"numbers.collect() = ${numbers.collect().mkString("[",",","]")}")
    //Done.
    spark.stop()
  }

}
