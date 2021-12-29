package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a filter() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDTransformationFilter {

  def ageFilter26(t3: (String, String, Int)): Boolean = {
    // t3 = (name, city, number)
    // name = t3._1
    // city = t3._2
    val age = t3._3
    if (age > 26) {
      // Keep the element
      return true
    } else {
      // Filter it out
      return false
    }
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * filter() transformation
     *
     *  filter(f)
     *  Description:
     *      Return a new RDD containing only the
     *      elements that satisfy a predicate.
     *
     * ----------------------------------------------------
     *
     * Create a list of tuples.
     * Each tuple contains name, city, and age.
     * Create a RDD from the list above.
     */
    val listOfTuples=
      List(("alex","Sunnyvale", 25),
        ("alex","Sunnyvale", 33),
        ("mary", "Ames", 22),
        ("mary", "Cupertino", 66),
        ("jane", "Ames", 20),
        ("bob", "Ames", 26))
    println("listOfTuples = " + listOfTuples)
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * apply a filter() transformation to rdd
     * and keep records if the age is > 26.
     * Use Lambda Expression
     *------------------------------------
     * t3._3 is the 3rd item of Tuple3
     */
    val filteredByLambda = rdd.filter(_._3 > 26)
    println("filteredByLambda = " + filteredByLambda)
    println("filteredByLambda.count() = " + filteredByLambda.count())
    println("filteredByLambda.collect() = " + filteredByLambda.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * apply a filter() transformation to rdd
     * and keep records if the age is > 26.
     * Use Scala Function
     *------------------------------------
     * t3._3 is the 3rd item of Tuple3
     */
    val filteredByFunction = rdd.filter(ageFilter26)
    println("filteredByFunction = " + filteredByFunction)
    println("filteredByFunction.count() = " + filteredByFunction.count())
    println("filteredByFunction.collect() = " + filteredByFunction.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
