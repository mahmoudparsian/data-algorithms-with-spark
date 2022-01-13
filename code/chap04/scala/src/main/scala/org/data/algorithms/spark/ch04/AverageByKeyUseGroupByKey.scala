package org.data.algorithms.spark.ch04

import org.apache.spark.sql.SparkSession

object AverageByKeyUseGroupByKey {
  def createSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("AverageByKeyUseReduceByKey")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    // create an instance of SparkSession
    val spark = createSparkSession
    val input = List(
      ("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),
      ("k2", 6), ("k2", 7), ("k2", 8),
      ("k3", 10), ("k3", 12)
    )
    // build RDD<key, value>
    val rdd = spark.sparkContext.parallelize(input)
    //group (key, value) pairs by key
    val groupByKey = rdd.groupByKey()
    //show grouped by key
    println(s"grouped_by_key = ${(groupByKey.mapValues(values => values.toList).collect()).mkString("["," , ","]")}")
    /*
     [
      ('k3', List(10, 12)),
      ('k2', List(6, 7, 8)),
      ('k1', List(1, 2, 3, 4, 5))
     ]
    */

    //find averages
    val avg = groupByKey.mapValues(values => values.sum.toFloat / values.size.toFloat )
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
