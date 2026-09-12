package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession
import org.data.algorithms.spark.ch01.AverageByKeyUseFoldByKey.createPair

/**
 *-----------------------------------------------------
 * Apply a reduceByKey() transformation to an
 * RDD[(key, value)] to find average per key.
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object AverageByKeyUseReduceByKey {

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
     *----------------------------------------
     * reduceByKey() transformation
     *
     * source_rdd.reduceByKey(func) --> target_rdd
     *
     * RDD<K,V> reduceByKey(Function2<V,V,V> func)
     * Merge the values for each key using an associative
     * and commutative reduce function. This will also
     * perform the merging locally on each mapper before
     * sending results to a reducer, similarly to a "combiner"
     * in MapReduce. Output will be hash-partitioned with the
     * existing partitioner/ parallelism level.
     *----------------------------------------
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
     *  apply a reduceByKey() transformation to rdd2
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
    val sumCount = rdd2.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

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
