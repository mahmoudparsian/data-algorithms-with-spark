package org.data.algorithms.spark.ch02

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.sys.exit

object DNSBaseCountVER2 {

  def processFASTAAsHashMap(fastaRecord: String) : TraversableOnce[(String,Int)] = {
    if(fastaRecord.startsWith(">"))
      return mutable.HashMap[String,Int]("z" -> 1)
    var hashmap = mutable.HashMap[String,Int]()
    var chars = fastaRecord.toLowerCase
    for(c<-chars)
      hashmap+=(c->1)
    var keyValueList = List.empty
      for (k <- hashmap)
        keyValueList += (k._1, k._2)

    return keyValueList
  }

  def main(args: Array[String]) = {
    if (args.length != 2) {
      println("Usage:" + DNABaseCountVER1 + "  <input-path> ")
      exit(-1)
    }
    //create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").appName("DNABaseCountVER2").getOrCreate()
    val inputPath = args(1)
    println("inputPath :" + inputPath)
    val recordsRDD = spark.sparkContext.textFile(inputPath)
    val pairsRDD = recordsRDD.flatMap(processFASTAAsHashMap)


  }

}
