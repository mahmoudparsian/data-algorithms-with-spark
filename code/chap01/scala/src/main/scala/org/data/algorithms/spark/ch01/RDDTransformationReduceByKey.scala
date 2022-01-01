package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession


/*
-----------------------------------------------------
 Apply a reduceByKey() transformation to an RDD
 Input: NONE
  ------------------------------------------------------
   Input Parameters:
      NONE
  -------------------------------------------------------
   @author Deepak Kumar
  -------------------------------------------------------
 */
object RDDTransformationReduceByKey {


  //=========================================
  def createPair(t3: (String, String, Int)): (String, Int) = {
    val name = t3._1
    val number = t3._3.toInt
    (name, number)
  } //end-def
  //=========================================

  def main(args: Array[String]) = {
    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    /*
    ========================================
     reduceByKey() transformation
    
     source_rdd.reduceByKey(func) --> target_rdd
    
     RDD<K,V> reduceByKey(Function2<V,V,V> func)
     Merge the values for each key using an associative
     and commutative reduce function. This will also
     perform the merging locally on each mapper before
     sending results to a reducer, similarly to a "combiner"
     in MapReduce. Output will be hash-partitioned with the
     existing partitioner/ parallelism level.
      ========================================

     Create a list of tuples.
     Each tuple contains name, city, and age.
     Create a RDD from the list above.
     */
    val listOfTuples = List(("alex", "Sunnyvale", 25),
      ("alex", "Sunnyvale", 33),
      ("alex", "Sunnyvale", 45),
      ("alex", "Sunnyvale", 63),
      ("mary", "Ames", 22),
      ("mary", "Cupertino", 66),
      ("mary", "Ames", 20),
      ("bob", "Ames", 26)
    )
    print("list of tuples= " + listOfTuples)
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " + rdd)
    println("rdd.count() = " + rdd.count())
    println("rdd.collect() = " + rdd.collect())
    rdd.collect.foreach(println _)
    /*
    ------------------------------------
     apply a map() transformation to rdd
     create a (key, value) pair
      where
           key is the name (first element of tuple)
           value is a number 
    ------------------------------------

    */
    val rdd2 = rdd.map(createPair)
    println("rdd2 = " + rdd2)
    println("rdd2.count() = " + rdd2.count())
    println("rdd2.collect() = " + rdd2.collect())
    rdd2.collect.foreach(println _)
    /*
    ------------------------------------
     ADD Numbers per key:
    
     apply a reduceByKey() transformation to rdd2
     create a (key, value) pair
      where
           key is the city name
           value is sum of number(s)
    ------------------------------------
    */
    val rdd3 = rdd2.reduceByKey((a, b) => a + b)
    println("rdd3 = " + rdd3)
    println("rdd3.count() = " + rdd3.count())
    println("rdd3.collect() = " + rdd3.collect())
    rdd3.collect.foreach(println _)

    /*
    ------------------------------------
     Find maximum per key:
    
     apply a reduceByKey() transformation to rdd2
     create a (key, value) pair
      where
           key is the city name
           value is sum of number(s)
    ------------------------------------

    */
    val rdd4 = rdd2.reduceByKey((a, b) => Math.max(a, b))
    println("rdd4 = " + rdd4)
    println("rdd4.count() = " + rdd4.count())
    println("rdd4.collect() = " + rdd4.collect())
    rdd4.collect.foreach(println _)
    //done!
    spark.stop()
  }

}
