package org.data.algorithms.spark.ch04

import org.apache.spark.sql.SparkSession

object AverageByKeyUseReduceByKey {

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
    // map each (key, value) into (key, (value, 1))
    val pairs = rdd.map( kv => (kv._1, (kv._2, 1)))
    /*
     pairs =
     [
      ("k1", (1, 1)), ("k1", (2, 1)), ("k1", (3, 1)), ("k1", (4, 1)), ("k1", (5, 1)),
      ("k2", (6, 1)), ("k2", (7, 1)), ("k2", (8, 1)),
      ("k3", (10, 1)), ("k3", (12, 1))
     ]

     reduce by key:
     x = (sum1, count1)
     y = (sum2, count2)
     x + y --> (sum1+sum2, count1+count2)
    */
    val sumCount = pairs.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    println(s"sum_count = ${sumCount.collect().mkString("[", ", ", "]")}")
    /*
     [
      ('k3', (22, 2)),
      ('k2', (21, 3)),
      ('k1', (15, 5))
     ]

     find averages
         v = (sum-of-values, count-of-values)
         v[0] = sum-of-values
         v[1] = count-of-values
    */
    val avg = sumCount.mapValues(v => (v._1/v._2).toFloat)
    print(s"avg = ${avg.collect().mkString("[", ", ", "]")}")
    /*
     avg.collect()
     [
      ('k3', 11),
      ('k2', 7),
      ('k1', 3)
     ]
    */

    // done!
    spark.stop()
  }
}
