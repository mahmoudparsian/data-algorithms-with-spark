package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a filter() to a DataFrame
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeFilter {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * filter(condition)
     * Filters rows using the given condition.
     *
     * where() is an alias for filter().
     *----------------------------------------
     */
    val triplets = List(("alex","Ames", 20),
      ("alex", "Sunnyvale",30),
      ("alex", "Cupertino", 40),
      ("mary", "Ames", 35),
      ("mary", "Stanford", 45),
      ("mary", "Campbell", 55),
      ("jeff", "Ames", 60),
      ("jeff", "Sunnyvale", 70),
      ("jane", "Austin", 80))

    println("triplets = " + triplets)
    import spark.implicits._
    val df = spark.createDataset(triplets).toDF("name", "city", "age")
    println("df.count(): " + df.count())
    df.show()
    df.printSchema()

    /**
     *-----------------------------------------
     * Filter Rows where age is greater than 50
     *-----------------------------------------
     */
    val df2 = df.filter(df.col("age")  > 50)
    println("df2.count(): " + df2.count())
    df2.show()
    df2.printSchema()

    /**
     *-----------------------------------------
     * Filter Rows where name contains 'me'
     *-----------------------------------------
     */

    val df3 = df.filter(df.col("city").contains("me"))
    println("df3.count(): " + df3.count())
    df3.show()
    df3.printSchema()

    // done!
    spark.stop()
  }
}
