package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Run SQL scripts after creating Dataframe
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DataframeSQL {
  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     *----------------------------------------
     * SparkSession.sql(sqlQuery)
     * sql(sqlQuery)
     *
     * Description:
     * Returns a DataFrame representing the result of the given query.
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
     * Register a DataFrame as a Table
     *-----------------------------------------
     */
    df.createOrReplaceTempView("people")

    /**
     *-----------------------------------------
     * Query: SELECT * FROM people
     *-----------------------------------------
     */
    val df2 = spark.sql("SELECT name, city, age FROM people")
    df2.show()

    /**
     *-----------------------------------------
     * Query: SELECT * FROM people where age > 62
     *-----------------------------------------
     */
    val df3 = spark.sql("SELECT name, city, age FROM people WHERE age > 62 ")
    df3.show()

    /**
     *-----------------------------------------
     * Query: SELECT name, count(*) FROM people GROUP BY "name"
     *-----------------------------------------
     */
    val df4 = df.groupBy("name").count()
    df4.show()

    /**
     *-----------------------------------------
     * Query: SELECT name, count(*) FROM people GROUP BY "name"
     *-----------------------------------------
     */
    val df5 = spark.sql("SELECT name, count(*) as namecount FROM people GROUP BY name")
    df5.show()

    // done!
    spark.stop()
  }
}
