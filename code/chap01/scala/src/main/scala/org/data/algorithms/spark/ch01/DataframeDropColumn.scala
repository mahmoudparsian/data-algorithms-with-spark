package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a drop() to a DataFrame:
 * drops an existing column from source DataFrame
 * and returns a new DataFrame
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeDropColumn {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * DataFrame.drop(*col)
     *
     * drop(*cols)
     * Returns a new DataFrame that drops the specified column.
     * This is a no-op if schema doesn't contain the given column
     * name(s).
     *
     * Parameters:
     *    cols - a string name of the column to drop, or a
     *           Column to drop, or a list of string name of
     *           the columns to drop.
     *
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
    val df = spark.createDataFrame(triplets).toDF("name", "city", "age")
    println("df.count(): " + df.count())
    df.show()
    df.printSchema()

    /**
     *-----------------------------------------
     * drop a column named 'city'
     *-----------------------------------------
     */
    val df2 = df.drop("city")
    df2.show()
    df2.printSchema()

    // done!
    spark.stop()
  }
}
