package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a new JDBC Table from an existing DataFrame
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
object DatasourceJDBCWriter {
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
     * Write to a JDBC table from a DataFrame
     *========================================
     *
     *
     *
     *
     * Use the SparkSession.createDataFrame() function to create a DataFrame.
     * In the following example, createDataFrame() takes a list of tuples containing
     * names, cities, and ages, and a list of column names:
     */
    val columnNames = List("name", "city", "age")
    val df = spark.createDataFrame(List(
      ("Alex", "Ames", 50),
      ("Gandalf", "Cupertino", 60),
      ("Thorin", "Sunnyvale", 95),
      ("Betty", "Ames", 78),
      ("Brian", "Stanford", 77)
    )).toDF(columnNames:_*)

    //
    println("df = " + df)
    println("df.count(): " + df.count())
    println("df.collect(): " + df.collect().mkString("Array(", ", ", ")"))
    df.show()
    df.printSchema()
    //

    //-----------------------------------
    // Write an existing DataFrame (df)
    // to a MySQL Database Table:
    //-----------------------------------
    df.write
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("driver", JDBC_DRIVER)
      .option("dbtable", JDBC_TARGET_TABLE_NAME)
      .option("user", JDBC_USER)
      .option("password", JDBC_PASSWORD)
      .mode("append")
      .save()

    // done!
    spark.stop()
  }
}
