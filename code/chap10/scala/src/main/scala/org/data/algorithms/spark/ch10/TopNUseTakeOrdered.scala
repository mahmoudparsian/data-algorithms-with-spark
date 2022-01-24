package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

import scala.util.Try

/*
-----------------------------------------------------
 Find Top-N per key
 Input: N
  ------------------------------------------------------
   Input Parameters:
      N
  -------------------------------------------------------
   @author Deepak Kumar
  -------------------------------------------------------
*/
object TopNUseTakeOrdered {
  def main(args: Array[String]): Unit = {
    if(args.length !=1){
      println("Usage: TopNUseTakeOrdered <N>")
      sys.exit(-1)
    }
    //create an instance of SparkSession
    val spark = SparkSession
      .builder()
      .appName("TopNUseTakeOrdered")
      .master("local[*]")
      .getOrCreate()
    println(s"spark =${spark}")
    //Top-N
    val N = Try(args(0).toInt) getOrElse 1
    println(s"N: ${N}")
    /*
    =====================================
    * Create an RDD from a list of values
    =====================================
    */
    val listOfKeys = List(
      ("a", 1), ("a", 7), ("a", 2), ("a", 3),
      ("b", 2), ("b", 4),
      ("c", 10), ("c", 50), ("c", 60), ("c", 70),
      ("d", 5), ("d", 15), ("d", 25),
      ("e", 1), ("e", 2),
      ("f", 9), ("f", 2),
      ("g", 22)
    )
    println(s"listOfKeys = ${listOfKeys}")
    /*
    *=====================================
    * create an RDD from a collection
    * Distribute a local Scala collection to form an RDD
    *=====================================
    */
    val rdd = spark.sparkContext.parallelize(listOfKeys)
    println("rdd= %s".format(rdd))
    println(s"rdd.count= ${rdd.count()}")
    println(s"rdd.collect()= ${rdd.collect().mkString("Array(", ", ", ")")}")
    /*
    *=====================================
    * Make sure same keys are combined 
    *====================================
    */
    val combined = rdd.reduceByKey((a, b) => a+b)
    println(s"combined= ${combined}")
    println(s"combined.count= ${combined.count()}")
    println(s"combined.collect()= ${combined.collect().mkString("Array(", ", ", ")")}")
    /*
    *=====================================
    * find Top-N
    *=====================================
    * x = (key, number)
    * x[0] --> key
    * x[1] --> number
    */
    val topN = combined.takeOrdered(N)(Ordering[Int].reverse.on(x=>x._2))
    println(s"topN = ${topN.mkString("Array(", ", ", ")")}")

    /*
    *=====================================
    * find Top-N
    *=====================================
    * x = (key, number)
    * x[0] --> key
    * x[1] --> number
    */
    val bottomN = combined.takeOrdered(N)(Ordering[Int].on(x=>x._2))
    println(s"bottomN = ${bottomN.mkString("Array(", ", ", ")")}")
  }
}
