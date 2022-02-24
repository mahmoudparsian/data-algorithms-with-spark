package org.data.algorithms.spark.bonuschapter

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/*
 * Find anagram counts for a given set of documents.
 * For example, if the sample input is comprised of
 * the following 3 lines:
 *
 *     Mary and Elvis lives in Detroit army Easter Listen
 *     silent eaters Death Hated elvis Mary easter Silent
 *     Mary and Elvis are in army Listen Silent detroit
 *
 * Then the output will be:
 *
 *     Sorted word      Anagrams and Frequencies
 *     -----------   -> ------------------------
 *     (adeht        -> {death=1, hated=1})
 *     (eilnst       -> {silent=3, listen=2})
 *     (eilsv        -> {lives=1, elvis=3})
 *     (aeerst       -> {eaters=1, easter=2})
 *     (amry         -> {army=2, mary=3})
  *
 * Since "in", "and", "are", "detroit" don't have
 * an associated anagrams, they will be filtered
 * out (dropped out):
 *
 *     in -> null
 *     are -> null
 *     and -> null
 *     Detroit -> null
 *
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Biman Mandal
 *
 */
object AnagramsByGroupByKey {

  def toWords(line: String) : Array[String] = {
    // replace all non alphanumeric with space
    line.replaceAll("[^0-9A-Za-z]+", " ")
      .toLowerCase
      //split by one or more spaces
      .split("\\s+")
  }

  def sortWord(x: String): String = {
    x.toCharArray.sorted.mkString
  }

  def listToDict: List[String] => Map[String, Int] =
    L => {
      val result = mutable.HashMap[String, Int]()
      L.foreach(ele => result.put(ele, result.getOrElse(ele, 0) + 1) )
      result.toMap
    }

  def filterRedundant(hashTable: Map[String, Int]): Boolean = hashTable.size > 1

  def main(args: Array[String]): Unit = {
    //
    val inputPath = args(0)
    println("inputPath=" + inputPath)

    // create an instance of a SparkSession as spark
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // create an RDD[String] for input
    val records = spark.sparkContext.textFile(inputPath);
    println("records=" + records.collect().mkString("[", ", ", "]"))

    // create RDD[(K, V)] from input
    // where K = sorted(word) and V = word
    val rdd = records.flatMap(toWords)
    println("rdd=" + rdd.collect().mkString("[", ", ", "]"))

    val rddKeyValue = rdd.map(x => (sortWord(x), x))

    // create anagrams as: RDD[(K, Iterable<String>)]
    val anagramsList = rddKeyValue.groupByKey()
    println("anagramsList=" + anagramsList.mapValues(l => l).collect().mkString("[", ", ", "]"))

    // anagrams: RDD[(K, Map<word, Integer>)]
    val anagrams = anagramsList.mapValues(l => listToDict(l.toList))
    println("anagrams=" + anagrams.collect().mkString("[", ", ", "]"))

    /**
     * filter out the redundant RDD elements:
     * now we should filter (k,v) pairs from anagrams RDD:
     * where k is a "sorted word" and v is a Map<String,Integer>
     * if len(v) == 1 then it means that there is no associated
     * anagram for the diven "sorted word".
     *
     * For example our anagrams will have the following RDD entry:
     * (k=Detroit, v=Map.Entry("detroit", 2))
     * since the size of v (i.e., the hash map) is one that will
     * be dropped out
     *
     * x:
     *    x[0]: sorted_word
     *    x[1]: Map<word, Integer>
     */
    val filteredAnagrams = anagrams.filter(x => filterRedundant(x._2))
    println("filteredAnagrams=")
    filteredAnagrams.collect().foreach(println)
  }

}

/*
records=[fox jumped bowel bowel bowel elbow below bare bear, fox jumped bore bore bore boer robe bears, bears baser saber fox jumped and jumped over bear, fox is silent and listen listen mars rams mars bears, Mary and Elvis lives in Detroit army Easter Listen, silent eaters Death Hated elvis Mary easter Silent, Artist Elvis are in army Listen Silent detroit, artist is here and strait and traits hated]
rdd=[fox, jumped, bowel, bowel, bowel, elbow, below, bare, bear, fox, jumped, bore, bore, bore, boer, robe, bears, bears, baser, saber, fox, jumped, and, jumped, over, bear, fox, is, silent, and, listen, listen, mars, rams, mars, bears, mary, and, elvis, lives, in, detroit, army, easter, listen, silent, eaters, death, hated, elvis, mary, easter, silent, artist, elvis, are, in, army, listen, silent, detroit, artist, is, here, and, strait, and, traits, hated]
anagramsList=[(aber,Seq(bare, bear, bear)), (adeht,Seq(death, hated, hated)), (is,Seq(is, is)), (eorv,Seq(over)), (beor,Seq(bore, bore, bore, boer, robe)), (aeerst,Seq(easter, eaters, easter)), (eehr,Seq(here)), (aer,Seq(are)), (fox,Seq(fox, fox, fox, fox)), (eilnst,Seq(silent, listen, listen, listen, silent, silent, listen, silent)), (adn,Seq(and, and, and, and, and)), (eilsv,Seq(elvis, lives, elvis, elvis)), (deiortt,Seq(detroit, detroit)), (below,Seq(bowel, bowel, bowel, elbow, below)), (in,Seq(in, in)), (amry,Seq(mary, army, mary, army)), (airstt,Seq(artist, artist, strait, traits)), (abers,Seq(bears, bears, baser, saber, bears)), (dejmpu,Seq(jumped, jumped, jumped, jumped)), (amrs,Seq(mars, rams, mars))]
anagrams=[(aber,Map(bear -> 2, bare -> 1)), (adeht,Map(death -> 1, hated -> 2)), (is,Map(is -> 2)), (eorv,Map(over -> 1)), (beor,Map(boer -> 1, robe -> 1, bore -> 3)), (aeerst,Map(eaters -> 1, easter -> 2)), (eehr,Map(here -> 1)), (aer,Map(are -> 1)), (fox,Map(fox -> 4)), (eilnst,Map(silent -> 4, listen -> 4)), (adn,Map(and -> 5)), (eilsv,Map(lives -> 1, elvis -> 3)), (deiortt,Map(detroit -> 2)), (below,Map(elbow -> 1, below -> 1, bowel -> 3)), (in,Map(in -> 2)), (amry,Map(army -> 2, mary -> 2)), (airstt,Map(traits -> 1, artist -> 2, strait -> 1)), (abers,Map(baser -> 1, saber -> 1, bears -> 3)), (dejmpu,Map(jumped -> 4)), (amrs,Map(rams -> 1, mars -> 2))]
filteredAnagrams=
(aber,Map(bear -> 2, bare -> 1))
(adeht,Map(death -> 1, hated -> 2))
(beor,Map(boer -> 1, robe -> 1, bore -> 3))
(aeerst,Map(eaters -> 1, easter -> 2))
(eilnst,Map(silent -> 4, listen -> 4))
(eilsv,Map(lives -> 1, elvis -> 3))
(below,Map(elbow -> 1, below -> 1, bowel -> 3))
(amry,Map(army -> 2, mary -> 2))
(airstt,Map(traits -> 1, artist -> 2, strait -> 1))
(abers,Map(baser -> 1, saber -> 1, bears -> 3))
(amrs,Map(rams -> 1, mars -> 2))
 */
