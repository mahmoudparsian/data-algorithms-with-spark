package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession
/**
 *-----------------------------------------------------
 * Write Content of RDD to file
 * Input: File Path
 *------------------------------------------------------
 * Input Parameters:
 *    Path
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object DatasourceTextfileWriter {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: DatasourceTextfileWriter <output-path>")
      System.exit(-1)
    }
    //create an instance of SparkSession
    val spark =
      SparkSession.
        builder().
        master("local[*]").
        getOrCreate()

    // read name of input file
    val outputPath = args(0)
    println(s"outputPath: ${outputPath}")
    /*
    ================================================
    # Create an RDD[String]
    ================================================
    */
    val data = List("data element 1", "data element 2", "data element 3", "data element 4")
    println(s"data = ${data}")
    val records = spark.sparkContext.parallelize(data)
    println(s"records = ${records}")
    println(s"records.count() = ${records.count()}")
    println(s"records.collect() = ${records.collect().mkString("[",",","]")}")
    /*
    #================================================
    # Save an RDD[String] to an output path
    #================================================
    */
    records.saveAsTextFile(outputPath)
    /*
    #================================================
    # read back from an output path and create and RDD[String]
      #================================================
    #
    */
    val loadedRecords = spark.sparkContext.textFile(outputPath)
    println(s"loaded_records = ${loadedRecords}")
    println(s"loaded_records.count() = ${loadedRecords.count()}")
    println(s"loaded_records.collect() = ${loadedRecords.collect().mkString("[",",","]")}")

    //Done.
    spark.stop()
  }

}
