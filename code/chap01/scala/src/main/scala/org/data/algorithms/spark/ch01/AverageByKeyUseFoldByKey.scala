package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Apply a foldByKey() transformation to an
 * RDD[key, value] to find average per key.
 *-------------------------------------------------------
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object AverageByKeyUseFoldByKey {

  def createPair(t3: (String, String, Int)): (String, (Int, Int)) = {
    // t3 = (name, city, number)
    val name = t3._1
    // city = t3._2
    val number = t3._3
    // return (k, v) pair
    // v = (sum, count)
    (name, (number, 1))
  }

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate();

    /**
     * ----------------------------------------------------
     * foldByKey() transformation:
     *
     * foldByKey(zeroValue)(func)
     *
     * Merge the values for each key using an associative
     * function “func” and a neutral "zeroValue" which may
     * be added to the result an arbitrary number of times,
     * and must not change the result (e.g., 0 for addition,
     * or 1 for multiplication.).
     *
     * source_rdd.foldByKey() --> target_rdd
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
        ("alex","Sunnyvale", 45),
        ("alex","Sunnyvale", 63),
        ("mary", "Ames", 22),
        ("mary", "Cupertino", 66),
        ("mary", "Ames", 20),
        ("bob", "Ames", 26))

    println("listOfTuples = " + listOfTuples)
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " +  rdd)
    println("rdd.count() = " + rdd.count())
    print("rdd.collect() = " + rdd.collect().mkString("Array(", ", ", ")"))

    /**
     * ----------------------------------------------
     *  apply a map() transformation to rdd
     *  create a (key, value) pair
     *   where
     *        key is the name (first element of tuple)
     *        value is a of tuple of (number, 1)
     * ----------------------------------------------
     */
    val rdd2 = rdd.map(createPair)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect().mkString("Array(", ", ", ")"))


    /**
     * ------------------------------------
     *  apply a foldByKey() transformation to rdd2
     *  create a (key, value) pair
     *   where
     *        key is the name
     *        value is the (sum, count)
     * ------------------------------------
     *  x = (sum1, count1)
     *  y = (sum2, count2)
     *  x._1+y._1 --> sum1+sum2
     *  x._2+y._2 --> count1+count2
     *
     *  zero_value = (0, 0) = (sum, count)
     */
    val sumCount = rdd2.foldByKey((0, 0))((x, y) => (x._1 + y._1, x._2 + y._2))

    println("sumCount = " + sumCount)
    println("sumCount.count() = " + sumCount.count())
    println("sumCount.collect() = " + sumCount.collect().mkString("Array(", ", ", ")"))

    // find average per key
    // sumCount = [(k1, (sum1, count1)), (k2, (sum2, count2)), ...]
    val averages = sumCount.mapValues(sumAndCount => sumAndCount._1.toFloat / sumAndCount._2.toFloat )
    println("averages = " + averages)
    println("averages.count() = " + averages.count())
    println("averages.collect() = " + averages.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}
