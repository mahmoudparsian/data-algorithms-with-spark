package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * map() is a 1-to-1 transformation
 * Apply a map() transformation to an RDD
 * Input: NONE
 *
 * print() is used for educational purposes.
 *------------------------------------------------------
 * Input Parameters:
 *    NONE
 *-------------------------------------------------------
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object MapTransformation1FromCollection {

  def createPair(t3: (String, String, Int)): (String, Int) = {
    // t3 = (name, city, age)
    val name = t3._1
    val age = t3._3
    (name, age)
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * map() transformation
     *
     * source_rdd.map(function) --> target_rdd
     *
     * map() is a 1-to-1 transformation
     *
     * map(f, preservesPartitioning=False)[source]
     * Return a new RDD by applying a function to each
     * element of this RDD.
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
        ("mary", "Ames", 20),
        ("bob", "Ames", 26))
    println("listOfTuples = " + listOfTuples)

    // create rdd : RDD[(name, city, age)]
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     *  apply a map() transformation to rdd
     *  create a (key, value) pair
     *   where
     *        key is the name (first element of tuple)
     *        value is the last element of tuple
     * ----------------------------------------------
     * rdd2: RDD[(name, age)]
     */
    val rdd2 = rdd.map(createPair)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------
     * increment age by 5
     *------------------------------------
     * rdd3 : RDD[(name, age)], where age is incremented by 5
     */
    val rdd3 = rdd2.mapValues(_ + 5)
    println("rdd3 = " + rdd3)
    println("rdd3.count() = " + rdd3.count())
    println("rdd3.collect() = " + rdd3.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

