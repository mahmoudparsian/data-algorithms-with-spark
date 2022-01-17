package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Read a JDBC Table and Create a DataFrame
 *
 * Input Parameters:
 *       JDBC_URL
 *       JDBC_DRIVER
 *       JDBC_USER
 *       JDBC_PASSWORD
 *       JDBC_TARGET_TABLE_NAME
 *------------------------------------------------------
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceJDBCReader {
  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    // "jdbc:mysql://localhost/metadb"
    val JDBC_URL = args(0)
    println("JDBC_URL = " + JDBC_URL)

    // "com.mysql.jdbc.Driver"
    val JDBC_DRIVER = args(1)
    println("JDBC_DRIVER = " + JDBC_DRIVER)

    // "root"
    val JDBC_USER = args(2)
    println("JDBC_USER = " + JDBC_USER)

    // "my-secret-pw"
    val JDBC_PASSWORD = args(3)

    // "people"
    val JDBC_TARGET_TABLE_NAME = args(4)
    println("JDBC_TARGET_TABLE_NAME = " + JDBC_TARGET_TABLE_NAME)

    /**
     *========================================
     * Read a JDBC table and create a DataFrame
     *========================================
     */
    val df = spark
      .read
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("driver", JDBC_DRIVER)
      .option("dbtable", JDBC_TARGET_TABLE_NAME)
      .option("user", JDBC_USER)
      .option("password", JDBC_PASSWORD)
      .load()

    //
    println("df = " + df)
    println("df.count(): " + df.count())
    println("df.collect(): " + df.collect().mkString("Array(", ", ", ")"))
    df.show()
    df.printSchema()

    // done!
    spark.stop()
  }
}
