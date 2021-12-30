package org.data.algorithms.spark.ch02

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.sys.exit

object DNSBaseCountVER2 {

  def processFASTAAsHashMap(fastaRecord: String) : List[(String,Int)] = {
    if(fastaRecord.startsWith(">"))
      return List[(String,Int)]("z" -> 1)
    var hashmap = mutable.HashMap[String,Int]()
    val chars = fastaRecord.toLowerCase

    for(c<-chars)
      hashmap.put(c.toString,hashmap.getOrElse(c.toString,0)+1)
    var keyValueList = hashmap.toList


    return keyValueList
  }

  def main(args: Array[String]) = {
    if (args.length != 2) {
      println("Usage:" + DNSBaseCountVER2 + "  <input-path> ")
      exit(-1)
    }
    //create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").appName("DNABaseCountVER2").getOrCreate()
    val inputPath = args(1)
    println("inputPath :" + inputPath)
    val recordsRDD = spark.sparkContext.textFile(inputPath)
    val pairsRDD = recordsRDD.flatMap(processFASTAAsHashMap)
    pairsRDD.collect().foreach(println)


  }

}
