package org.data.algorithms.spark.ch04

import org.apache.spark.sql.SparkSession

object AverageByKeyUseCombineByKey {
  def createSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("AverageByKeyUseReduceByKey")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    //Create a new spark session
    val spark = createSparkSession
    val input = List(
      ("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),
      ("k2", 6), ("k2", 7), ("k2", 8),
      ("k3", 10), ("k3", 12)
    )
    // build RDD<key, value>
    val rdd = spark.sparkContext.parallelize(input)
    /*
     Combined data structure (C) is a Tuple2(sum, count)
     3 functions needs to be defined:
        v --> C
        C, v --> C
        C, C --> C
    */
    val sumCount = rdd.combineByKey(
      v=> (v,1),
      (C:(Int,Int),v:Int) => (C._1+v , C._2 + 1) ,
      (C1:(Int,Int),C2:(Int,Int)) => (C1._1+C2._1,C1._2+C2._2)
    )
    //show sum count
    println(s"sum_count = ${sumCount.collect().mkString("["," ,","]")}")
    /*
     [
      (k3, (22, 2)),
      (k2, (21, 3)),
      (k1, (15, 5))
     ]
    */

    //find averages
    val avg = sumCount.mapValues(values => (values._1/values._2).toFloat)
    println(s"avg = ${avg.collect().mkString("["," ,","]")}")
    /*
     avg.collect()
     [
      (k3, 11.0),
      (k2, 7.0),
      (k1, 3.0)
     ]
    */
    //Done!
    spark.stop()
  }
}
