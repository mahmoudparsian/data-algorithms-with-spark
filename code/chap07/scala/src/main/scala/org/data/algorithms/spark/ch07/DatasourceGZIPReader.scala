package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create an RDD from a .gz files (which
 * may have any number of files in it)
 * Input: .gz File(s)
 *------------------------------------------------------
 * Input Parameters:
 *    Zipped File
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceGZIPReader {

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    // read name of input file(s)
    val gzInputPath = args(0)
    println("gzInputPath : " + gzInputPath)

    /**
     *=====================================
     * Create an RDD from a given .gz file(s)
     *=====================================
     */
    val gzipRdd = spark.sparkContext.textFile(gzInputPath)

    println("gzipRdd = " + gzipRdd)

    println("gzipRdd.count() = " + gzipRdd.count())

    println("gzipRdd.collect() = " + gzipRdd.collect().mkString("Array(", ", ", ")"))

  }
}
