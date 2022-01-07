package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a join()
 * source_df.join(other_df, "right")
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeJoinRight {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * join(otherDF, on, how)
     *
     * Joins with another DataFrame, using the given
     * join expression.
     *
     * Parameters:
     *  other - Right side of the join
     *  on - a string for the join column name,
     *       a list of column names, a join
     *       expression (Column), or a list of Columns.
     *       If on is a string or a list of strings
     *       indicating the name of the join column(s),
     *       the column(s) must exist on both sides, and
     *       this performs an equi-join.
     *  how - str, default inner. Must be one of:
     *        inner, cross, outer, full, full_outer, left,
     *        left_outer, right, right_outer, left_semi,
     *        and left_anti.
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
    val df = spark.createDataset(triplets).toDF("name", "city", "age")
    println("df.count(): " + df.count())
    df.show()
    df.printSchema()

    val triplets2 =
       List(("david", "software"),
        ("david", "business"),
        ("terry", "coffee"),
        ("terry", "hardware"),
        ("mary", "marketing"),
        ("mary", "sales"),
        ("jane", "genomics"))


    println("triplets2 = " + triplets2)
    val df2 = spark.createDataset(triplets2).toDF("name", "dept")
    println("df2.count(): " + df2.count())
    df2.show()
    df2.printSchema()

    /**
     *-----------------------------------------
     * df.join(df2)
     *-----------------------------------------
     */
    val joined = df.join(df2, df.col("name") === df2.col("name"), "right")
    joined.show()
    joined.printSchema()

    // done!
    spark.stop()
  }
}
