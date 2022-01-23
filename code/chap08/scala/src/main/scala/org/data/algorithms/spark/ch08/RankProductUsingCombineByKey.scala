package org.data.algorithms.spark.ch08

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import scala.Console.{BOLD, RED, RESET}
import scala.math.pow
import scala.util.Try

/**
 * -----------------------------------------------------
 *RankProductUsingCombineByKey.scala
 *NOTE: combineByKey() is used for grouping
 *keys by their associated values.
 *Handles multiple studies, where each study is
 *a set of assays.  For each study, we find the
 *mean per gene and then calculate the rank product
 *for all genes.
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object RankProductUsingCombineByKey {

  def createPair(urls: String):(String,Double) = {
    // Parses a urls pair string into urls pair.
    val tokens = urls.split(",")
    val sourceURL = tokens(0)
    val targetURL = tokens(1).toDouble
    (sourceURL,targetURL)
  }

  /*
  Compute mean per gene for a single study = set of assays
  @param input_Path set of assay paths separated by ","
  @RETURN RDD[(String, Double)]
  */
  def computeMean(inputPath: String,spark:SparkSession):RDD[(String,Float)] = {
    println(s"Input Path ${inputPath}")
    //genes as string records: RDD[String]
    val rawGenes = spark.sparkContext.textFile(inputPath)
    print(s"Raw Genes ${rawGenes.collect().mkString("[",",","]")}")
    //create RDD[(String, Double)]=RDD[(gene_id, test_expression)]
    val genes = rawGenes.map(createPair)
    print(s"genes ${genes.collect().mkString("[",",","]")}")

    //create RDD[(gene_id, (sum, count))]
    val genesCombined = genes.combineByKey[(Double, Double)](
      v => (v,1.0),
      (C,v) => (C._1+v,C._2+1),
      (C,D) => (C._1+D._1,C._2+D._2)
    )
    println(s"Genes Combined ${genesCombined.collect().mkString("[",",","]")}")
    //now compute the mean per gene
    val genesMean = genesCombined.mapValues(p => p._1.toFloat/p._2.toFloat)
    genesMean

  }

  /*
  @param rdd : RDD[(String, Double)]
    returns: RDD[(String, Long)] : (gene_id, rank)
  */
  def assignRanks(rdd:RDD[(String,Float)]): RDD[(String, Long)] = {

    //swap key and value (will be used for sorting by key)
    //convert value to abs(value)
    val swappedRDD = rdd.map(v => (v._2.abs,v._1))

    /*
     sort copa scores descending
     we need 1 partition so that we can zip numbers into this RDD by zipWithIndex()
     If we do not use 1 partition, then indexes will be meaningless
     sorted_rdd : RDD[(Double,String)]
    */
    val sortedRDD = swappedRDD.sortByKey(false,1)
    println(s"sortedRDD ${sortedRDD.collect().mkString("Array(", ", ", ")")}")

    /*
     use zipWithIndex()
     Long values will be 0, 1, 2, ...
     for ranking, we need 1, 2, 3, ..., therefore,
     we will add 1 when calculating the ranked product
     indexed :  RDD[((Double,String), Long)]
    */
    val indexed = sortedRDD.zipWithIndex()
    println(s"Indexed ${indexed.collect().mkString("[",",","]")}")

    //add 1 to index
    //ranked :  RDD[(String, Long)]
    val ranked = indexed.map(v=> (v._1._2, v._2+1))
    print("ranked", ranked.collect().mkString("Array(", ", ", ")"))
    return ranked
  }

  /*
  return RDD[(String, (Double, Integer))] = (gene_id, (ranked_product, N))
  where N is the number of elements for computing the ranked product
  @param ranks: RDD[(String, Long)][]
  */
  def computeRankedProducts(ranks: List[RDD[(String,Long)]],spark: SparkSession): RDD[(String, (Double, Int))] = {
    //combine all ranks into one
    val unionRDD = spark.sparkContext.union(ranks)

    /*
    next find unique keys, with their associated copa scores
    we need 3 function to be able to use combinebyKey()
    combinedByGene: RDD[(String, (Double, Integer))]
    */
    val combinedByGene = unionRDD.combineByKey[(Long,Int)](
      v => (v,1),
      (C,v) => (C._1*v,C._2+1),
      (C,D) => (C._1*D._1,C._2*C._2)
    )

    //next calculate ranked products and the number of elements
    val rankedProducts = combinedByGene.mapValues(
      v => (pow(v._1.toFloat, v._2.toFloat), v._2)
    )

    return rankedProducts
  }

  /**
   * -------------------------------------------
   * Input parameters:
   *        args(0)   = output path
   *        args(1)   = number of studies (K)
   *        args(2)   = input path for study 1
   *        args(3)   = input path for study 2
   *        ...
   *        args(n) = input path for study K
  */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Rank Product").master("local[*]").getOrCreate()
    //Handle input parameters
    val outputPath = Try(args(0)) getOrElse s"${RED}${BOLD}No outputPath Provided${RESET}"
    println(s"outputPath ${outputPath}")
    //set K = number of studies
    val K = Try(args(1).toInt-1) getOrElse 3
    println(s"K ${K}")
    //define studies_input_path
    val studiesInputPath = (for(l <- 0 to K) yield args(l+2)).toList
    println(studiesInputPath)

    /*
    Step-1: Perform Rank Product
     Spark requires an array for creating union of many RDDs
    means(i) : RDD[(String, Double)]
    */
    val means =  for(i <- 0 to K) yield computeMean(studiesInputPath(i),spark)

    /*
    * Step-2: Compute rank
    *   1. sort values based on absolute value of copa scores:
    *   to sort by copa score, we will swap K with V and then sort by key
    *   2. assign rank from 1 (to highest copa score) to n (to the lowest copa score)
    *   3. calcluate rank for each gene_id as Math.power(R1 * R2 * ... * Rn, 1/n)
    */
    val ranks = for(i <- 0 to K) yield assignRanks(means(i))

    /*
    Step-3: Calculate ranked products
      ranked_products  : RDD[(gene_id, (ranked_product, N))]
    */
    val rankedProducts = computeRankedProducts(ranks.toList,spark)
    //Step-4: save the result
    rankedProducts.saveAsTextFile(outputPath)
    println()
    //Done!
    spark.stop()
    

  }
}
