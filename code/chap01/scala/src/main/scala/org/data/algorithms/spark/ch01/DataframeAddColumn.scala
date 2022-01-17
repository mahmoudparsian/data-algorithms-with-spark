package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a withColumn() to a DataFrame:
 * add a new column to source DataFrame
 * and return a new DataFrame
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeAddColumn {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * DataFrame.withColumn(colName, col)
     *
     * Returns a new DataFrame by adding a column
     * or replacing the existing column that has
     * the same name.
     *
     * The column expression must be an expression
     * over this DataFrame; attempting to add a column
     * from some other dataframe will raise an error.
     *
     * Parameters:
     *   colName - string, name of the new column.
     *   col - a Column expression for the new column.
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
     * add a new column as age2
     *-----------------------------------------
     */
    val df2 = df.withColumn("age2", df.col("age") + 2)
    df2.show()
    df2.printSchema()

    // done!
    spark.stop()
  }
}
