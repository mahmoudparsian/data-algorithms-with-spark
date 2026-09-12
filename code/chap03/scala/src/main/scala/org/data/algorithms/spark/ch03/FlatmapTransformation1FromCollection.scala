package org.data.algorithms.spark.ch03

import org.apache.spark.sql.SparkSession

object FlatmapTransformation1FromCollection {
  def main(args: Array[String]): Unit = {
    //create spark session
    val spark =
      SparkSession.builder()
        .appName("FlatmapTransformation1FromCollection")
        .master("local[*]")
        .getOrCreate()
    /*
    ========================================
     flatMap() transformation
    
     source_rdd.flatMap(function) --> target_rdd
    
     flatMap() is a 1-to-Many transformation
    
     Spark's flatMap() is a transformation operation that flattens
     the RDD/DataFrame (array/map DataFrame columns) after applying the
     function on every element and returns a new PySpark RDD/DataFrame.
    
    ========================================
    */
    val listOfArrays = List(
                          List("item_11","item_12", "item_13"),
                          List("item_21","item_22"),
                          List.empty,
                          List("item_31","item_32", "item_33", "item_34"),
                          List.empty
                       )
    println(s"listOfarrays = $listOfArrays")
    // create rdd : RDD[(name, city, age)]
    val rdd = spark.sparkContext.parallelize(listOfArrays)
    println(s"rdd = ${rdd}")
    println(s"rdd.count() = ${rdd.count()}")
    println(s"rdd.collect() = ${rdd.collect().mkString("(", ", ", ")")}")
    /*
    ------------------------------------
     apply a map() transformation to rdd
      ------------------------------------
     rdd2 : RDD[[String]]
    */
    val rddMapped = rdd.map(x => x)
    /*
    ------------------------------------
     apply a flatMap() transformation to rdd
     Note-1: empty lists will be dropped
       Note-2: [str1, str2, str3] will be flattened
     and mapped to 3 target elements as str1, str2, and str3
      ------------------------------------
     rdd_flatmapped : RDD[String]
    */
    val rddFlatmapped = rddMapped.flatMap( t => t)
    println(s"rdd_flatmapped = ${rddFlatmapped}")
    println(s"rdd_flatmapped.count() = ${rddFlatmapped.count()}")
    println(s"rdd_flatmapped.collect() = ${rddFlatmapped.collect().mkString("[", ", ", "]")}")
  }

}
