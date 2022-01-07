package org.data.algorithms.spark.ch01

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object AggregateByKeyUseCombineByKey {

  /*  -----------------------------------------------------
     Apply a combineByKey() transformation to an
     RDD[(key, value)] to find average per key.
     Input: NONE
       Create RDD[(key, value)] from a Collection
    ------------------------------------------------------
     Input Parameters:
        NONE
    -------------------------------------------------------
     @author Deepak Kumar
    -------------------------------------------------------
    =========================================
    Create a pair of (name, number) from t3
    t3 = (name, city, number)*/
  def createPair(t3: (String, String, Int)): (String, Int) = {
    val name = t3._1
    val number = t3._3.intValue()
    (name, number)
  }
  /* end-def
  ==========================================*/

  def main(args: Array[String]) = {
    //create an instance of SparkSession
    val spark = SparkSession
      .builder()
      .appName("Combine By Key")
      .master("local[*]")
      .getOrCreate()
    /*========================================
   combineByKey() transformation
  
   source_rdd.combineByKey(lambda1, lambda2, lambda3) --> target_rdd
  
   RDD<K,C> combineByKey(
                         Function<V,C> createCombiner,
                         Function2<C,V,C> mergeValue,
                         Function2<C,C,C> mergeCombiners
                        )
  
   Simplified version of combineByKey that hash-partitions the
   resulting RDD using the existing partitioner/parallelism level
   and using map-side aggregation.
  ========================================

   Create a list of tuples.
   Each tuple contains name, city, and age.
   Create a RDD from the list above.*/
    val listOfTuples = List(("alex", "Sunnyvale", 25),
      ("alex", "Sunnyvale", 33),
      ("alex", "Sunnyvale", 45),
      ("alex", "Sunnyvale", 63),
      ("mary", "Ames", 22),
      ("mary", "Cupertino", 66),
      ("mary", "Ames", 20),
      ("bob", "Ames", 26)
    )
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " + rdd)
    println("rdd.collect()=" + rdd.collect().mkString("Array(", ", ", ")"))
    println("rdd.count()=" + rdd.count())
    rdd.collect.foreach(println)

    /* ------------------------------------
    apply a map() transformation to rdd
    create a (key, value) pair
     where
          key is the name (first element of tuple)
          value is a number
     ------------------------------------*/
    val rdd2 = rdd.map(createPair)
    println("rdd2 = ", rdd2)
    println("rdd2.count() = ", rdd2.count())
    println("rdd2.collect() = ", rdd2.collect().mkString("Array(", ", ", ")"))

    /* ------------------------------------
    apply a combineByKey() transformation to rdd2
    create a (key, value) pair
     where
          key is the name
          value is the (sum, count)
   ------------------------------------
    v : a number for a given (key, v) pair of source RDD
    C : is a tuple of pair (sum, count), which is
    also called a "combined" data structure
    C1 and C2 are a "combined" data structure:
      C1 = (sum1, count1)
      C2 = (sum2, count2)*/
    val createCountCombiner = (v: Double) => (v, 1)
    //(alex,(25,1))
    val sumCount = rdd2.combineByKey[(Int, Int)](v => (v, 1), (C, v) => (C._1 + v, C._2 + 1), (C1, C2) => (C1._1 + C2._1, C1._2 + C2._2))
    sumCount.collect.foreach(println(_))
    //done!
    spark.stop()

  }
}
