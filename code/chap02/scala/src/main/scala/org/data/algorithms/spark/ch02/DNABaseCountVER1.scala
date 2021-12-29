package org.data.algorithms.spark.ch02

import org.apache.spark.sql.SparkSession

import scala.sys.exit

/*
-----------------------------------------------------
 Version-1
 This is a DNA-Base-Count in PySpark.
 The goal is to show how "DNA-Base-Count" works.
------------------------------------------------------
 Input Parameters:
      argv[1]: String, input path
  -------------------------------------------------------
   @author Deepak Kumar
  -------------------------------------------------------
*/
object DNABaseCountVER1 {

  def processFASTARecord(fastaRecord: String): Map[String, Int] = {
    var keyValueList = Map[String, Int]()
    if (fastaRecord.startsWith(">"))
      keyValueList += ("z" -> 1)
    else {
      var chars = fastaRecord.toLowerCase
      for (c <- chars)
        keyValueList += c.toString -> 1
    }
    return keyValueList
  }

  def main(args: Array[String]) = {
    if (args.length != 2) {
      println("Usage:" + DNABaseCountVER1 + "  <input-path> ")
      exit(-1)
    }
    //create an instance of SparkSession object
    val spark = SparkSession.builder().appName("DNABaseCountVER1").master("local[*]").getOrCreate()
    val inputPath = args(1)
    println("inputPath :" + inputPath)
    val recordsRDD = spark.sparkContext.textFile(inputPath)
    println("recordsRDD.count() : " + recordsRDD.count())
    val recordsAsList = recordsRDD.collect()
    print("recordsAsList : ", recordsAsList)
    // if you do not have enough RAM, then do the following
    // MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
    //recordsRDD.persist(StorageLevel(True, True, False, False, 1))
    //
    val pairsRDD = recordsRDD.flatMap(processFASTARecord)
    pairsRDD.collect.foreach(println)

    val frequenciesRDD = pairsRDD.reduceByKey((x, y) => (x + y))
    println("frequenciesRDD : debug")
    val frequenciesAsList = frequenciesRDD.collect()
    println("frequenciesAsList : " + frequenciesAsList.foreach(println))
  }

}
