package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a describe() action to a DataFrame
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeActionDescribe {
  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * DataFrame.describe() action
     *
     * Description:
     * Computes basic statistics for numeric and string columns.
     * This include count, mean, stddev, min, and max. If no columns
     * are given, this function computes statistics for all numerical
     * or string columns.
     *----------------------------------------
     */
    val pairs = List((10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9"))
    println("pairs = " + pairs)
    import spark.implicits._
    val df = spark.createDataset(pairs).toDF( "number", "name")
    println("df.count(): " + df.count())
    println("df.collect(): " + df.collect().mkString("Array(", ", ", ")"))
    df.show()

    /**
     *-----------------------------------------
     * apply describe() action
     *-----------------------------------------
     */
    df.describe("number").show()
    df.describe().show()

    // done!
    spark.stop()
  }
}
