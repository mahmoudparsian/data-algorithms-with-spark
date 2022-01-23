package org.data.algorithms.spark.ch08

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 *-----------------------------------------------------
 * This is an example implementation of PageRank.
 * For more conventional use, please refer to the
 * PageRank implementation provided by graphframes
 * and graphx.
 *------------------------------------------------------
 * Input Data Format:
 *   <source-URL-ID><,><neighbor-URL-ID>
 *  Example Usage:
 * export num_of_iterations=20
 * export input_path="pagerank_data.txt"
 * spark-submit pagerank.py $input_path $num_of_iterations
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object PageRank {

  def createPair(urls: String):(String,String) = {
    // Parses a urls pair string into urls pair.
    val tokens = urls.split(",")
    val sourceURL = tokens(0)
    val targetURL = tokens(1)
    (sourceURL,targetURL)
  }

  def printRanks(ranks: RDD[(String, Double)]): Unit = {
    ranks.collect().sorted.foreach(x=>println(f"${x._1}%s has ${x._2}%2.2f rank"))
  }

  def computeContributions(urlsRank:(String, (Iterable[String], Double))): Iterable[(String, Double)] = {
    // Calculates URL contributions to the rank of other URLs.
    val urls = urlsRank._2._1
    val rank = urlsRank._2._2
    val numURLs = urls.size
    for(url <- urls)
      yield(url,rank/numURLs)
  }

  def recalculateRank(rank:Double): Double = {
    val newRank = rank * 0.85 + 0.15
    newRank
  }

  def main(args: Array[String]): Unit = {
    //STEP-1: Read input parameters:
    val inputPath = args(0)
    val numOfIterations = Try(args(1).toInt) getOrElse(5)
    //STEP-2: Initialize the spark session.
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    //STEP-3: Create RDD[String] from input_path
    val records = spark.sparkContext.textFile(inputPath)
    //STEP-4: Create initial links
    //Loads all URLs from input file and initialize their neighbors.
    val links = records.map(rec=>createPair(rec)).distinct().groupByKey().cache()
    println(s"links : ${links.collect().mkString("[",",","]")}")
    //STEP-5: Initialize ranks to 1.0
    //Loads all URLs with other URL(s) link to from
    //input file and initialize ranks of them to one.
    var ranks = links.map(urlNeighbors => (urlNeighbors._1,1.0))
    println(s"ranks : ${ranks.collect().mkString("[",",","]")}")
    //STEP-6: Perform iterations...
    //Calculates and updates URL ranks continuously using PageRank algorithm.
    for(iteration <- 1 to numOfIterations) {
      //debug ranks
      printRanks(ranks)
      //Calculates URL contributions to the rank of other URLs.
      val contributions = links.join(ranks).flatMap(computeContributions)
      println()
      //Re-calculates URL ranks based on neighbor contributions.
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(recalculateRank)
    }
    //STEP-7: Display the page ranks
    printRanks(ranks)
    //Done.
    spark.stop()
  }

}

/*
links : [(url_4,Seq(url_3, url_1)),(url_2,Seq(url_1)),(url_3,Seq(url_2, url_1)),(url_1,Seq(url_4))]
ranks : [(url_4,1.0),(url_2,1.0),(url_3,1.0),(url_1,1.0)]
url_1 has 1.00 rank
url_2 has 1.00 rank
url_3 has 1.00 rank
url_4 has 1.00 rank

url_1 has 1.85 rank
url_2 has 0.58 rank
url_3 has 0.58 rank
url_4 has 1.00 rank

url_1 has 1.31 rank
url_2 has 0.39 rank
url_3 has 0.58 rank
url_4 has 1.72 rank

url_1 has 1.46 rank
url_2 has 0.39 rank
url_3 has 0.88 rank
url_4 has 1.26 rank

url_1 has 1.40 rank
url_2 has 0.52 rank
url_3 has 0.69 rank
url_4 has 1.39 rank

url_1 has 1.48 rank
url_2 has 0.44 rank
url_3 has 0.74 rank
url_4 has 1.34 rank
*/

