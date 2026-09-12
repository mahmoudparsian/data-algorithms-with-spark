package org.data.algorithms.spark.ch04

import org.apache.spark.sql.functions.{col, collect_list, rand, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
-----------------------------------------------------
 This program find the exact median per key.

------------------------------------------------------
 Note-1: print(), collect(), and show() are used
   for debugging and educational purposes only.
  
  ------------------------------------------------------
   Input Parameters:
      none
  -------------------------------------------------------
  
   @author Deepak Kumar
  -------------------------------------------------------
*/
object DataframeMedianExact {

  def createTestDataframe(sparkSession: SparkSession, numOfKeys: Int, numOfRows: Int): DataFrame = {
    val key = (col("id") % numOfKeys).alias("key")
    val value = (rand(41)+key * numOfKeys).alias("value")
    val df = sparkSession.range(0,numOfRows,1,1).select(key,value)
    return df
  }

  def calculateMedian(L: Seq[Float]) : Float = {
    breeze.stats.median(L)
  }

  def main(args: Array[String]): Unit = {
    //create an instance of spark session
    val spark =
      SparkSession.builder()
        .appName("DataframeMedianExact")
        .master("local[*]")
        .getOrCreate()
    /*
     create a DataFrame with 1000,000 rows and two columns: "key" and "value"
     number of keys will be 10 {0, 1, 2,, ..., 9}
    */
    val df = createTestDataframe(spark, 10, 1000000)
    print("df.count()=", df.count())
    df.printSchema()
    df.show(20, truncate=false)
    /*
     create a UDF from a scala function:
     FloatType() is a return type of function calculate_median(list)
    */
    val calculateMedianUDF = udf( L => calculateMedian(L))
    // relative error = 1/10,000,000
    // use approximation df2.agg(collect_list('age'))
    val exactMedianPerKey = df.groupBy("key").agg(calculateMedianUDF(collect_list("value")).alias("median"))
    print("exact_median_per_key.count()=", exactMedianPerKey.count())
    exactMedianPerKey.printSchema()
    exactMedianPerKey.show(truncate=false)
    //Done
    spark.stop()
  }

}
