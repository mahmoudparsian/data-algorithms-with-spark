package org.data.algorithms.spark.bonuschapter

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 *
 *
 * This Spark code provides K-mer counting functionality.
 *
 * Kmer counting for a given K and N, where
 * K: to find K-mers
 * N: to find top-N
 *
 * FASTA format: https://en.wikipedia.org/wiki/FASTA_format
 * A k-mer (or kmer) is a short DNA sequence consisting of
 * a fixed number (K) of bases. The value of k is usually
 * divisible by 4 so that a kmer can fit compactly into a
 * basevector object. Typical values include 12, 20, 24, 36,
 * and 48; kmers of these sizes are referred to as 12-mers,
 * 20-mers, and so forth.
 *
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Biman Mandal
 *
 * */
object KMERFastQ {

  // ====================================================
  //  drop non DNA sequences
  def filterRecords(rec: String): Boolean = {
    if (rec == null) return false
    val strippedRec = rec.strip()
    if (strippedRec.length < 1) return false
    if (Set('@', '+', ';', '!', '~').contains(strippedRec.charAt(0))) return false
    true
  }

  def generateKmers(dna: String, KB: Broadcast[Int]): List[(String, Int)] = {
    val k = KB.value
    if (dna.length < k)  return List.empty
    val listOfPairs = ListBuffer[(String, Int)]()
    for (x <- 0 until dna.length + 1 - k) {
      val kmer = dna.substring(x, x + k)
      listOfPairs.append((kmer, 1))
    }
    listOfPairs.toList
  }

  def main(args: Array[String]): Unit = {
    //  define input parameters: FASTA format
    val fastaInputPath = args(0)
    val K = args(1).toInt //  to find K-mers
    val N = args(2).toInt //  to find top-N
    //
    println("K=" + K)
    println("N=" + N)
    println("fastaInputPath=" + fastaInputPath)


    //  create an instance of SparkSession
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    println("spark.version=" + spark.version)

    //  make K as a distributed data/value
    val KB = sc.broadcast(K)
    println("KB.value=" + KB.value)

    //  read input and create the first RDD  as RDD[String]
    val records = sc.textFile(fastaInputPath)
    println("records=" + records.collect().mkString("Array(", ", ", ")"))

    //  drop records, which are not DNA sequences
    val sequences = records.filter(filterRecords)
    println("sequences=" + sequences.collect().mkString("Array(", ", ", ")"))

    //  generate K-mers pairs: (k-mer, 1)
    val pairs = sequences.flatMap(seq =>  generateKmers(seq, KB))
    println("pairs=" + pairs.collect().mkString("Array(", ", ", ")"))

    //  find frequency of kmers
    val frequencies = pairs.reduceByKey((x,y) => x + y)
    println("frequencies=" + frequencies.collect().mkString("Array(", ", ", ")"))

    //  emit final topN descending
    val topN = frequencies.takeOrdered(N)(Ordering[Int].reverse.on(x => x._2))
    println("topN=" + topN.mkString("Array(", ", ", ")"))

    //  done
    spark.stop()
  }

}

/*
records=Array(@SEQ_ID, GATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT, +, !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65, @SEQ_ID, GATTTCCCGTTCAAAGCAGTATCGATCTTTTAGTAAATCCATTTGTTCAACTCACAGTTG, +, !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65, @SEQ_ID, GACCCGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT, +, !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65, @SEQ_ID, TCATCATCATCCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT, +, !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65, @SEQ_ID, AGTAAGTAAGTAATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCTAGTAAGTA, +, !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65)
sequences=Array(GATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT, GATTTCCCGTTCAAAGCAGTATCGATCTTTTAGTAAATCCATTTGTTCAACTCACAGTTG, GACCCGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT, TCATCATCATCCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT, AGTAAGTAAGTAATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCTAGTAAGTA)
pairs=Array((GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (GATT,1), (ATTT,1), (TTTC,1), (TTCC,1), (TCCC,1), (CCCG,1), (CCGT,1), (CGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCT,1), (TCTT,1), (CTTT,1), (TTTT,1), (TTTA,1), (TTAG,1), (TAGT,1), (AGTA,1), (GTAA,1), (TAAA,1), (AAAT,1), (AATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTG,1), (GACC,1), (ACCC,1), (CCCG,1), (CCGG,1), (CGGG,1), (GGGG,1), (GGGT,1), (GGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTA,1), (GTAA,1), (TAAA,1), (AAAT,1), (AATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGC,1), (AGCC,1), (GCCT,1), (TCAT,1), (CATC,1), (ATCA,1), (TCAT,1), (CATC,1), (ATCA,1), (TCAT,1), (CATC,1), (ATCC,1), (TCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTA,1), (GTAA,1), (TAAA,1), (AAAT,1), (AATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGC,1), (AGCC,1), (GCCT,1), (AGTA,1), (GTAA,1), (TAAG,1), (AAGT,1), (AGTA,1), (GTAA,1), (TAAG,1), (AAGT,1), (AGTA,1), (GTAA,1), (TAAT,1), (AATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTA,1), (GTAA,1), (TAAA,1), (AAAT,1), (AATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGC,1), (AGCC,1), (GCCT,1), (CCTA,1), (CTAG,1), (TAGT,1), (AGTA,1), (GTAA,1), (TAAG,1), (AAGT,1), (AGTA,1))
frequencies=Array((GGGC,1), (TTTT,1), (CAAC,5), (ACCC,1), (CAGC,3), (CGGG,1), (GTTG,1), (CCGG,1), (GCCC,1), (TTCA,7), (AGTT,2), (CGTT,1), (GATT,2), (TCAT,3), (TAAT,1), (GCAG,4), (CCCG,2), (GACC,1), (GGCC,1), (TAGT,6), (AGCA,4), (ATCT,1), (CAAA,8), (AGCC,3), (TTCC,1), (GGGG,2), (ACAG,5), (CACA,5), (AAAG,4), (CATT,5), (CCAA,2), (CCCA,1), (AAGC,4), (GGTT,1), (TATC,4), (GTAT,4), (TTGG,1), (TTAG,1), (GTTC,7), (ATAG,4), (TGGG,1), (TTGT,5), (CCTA,1), (TTTC,1), (CATC,3), (GTTT,1), (CCAT,5), (CTAG,1), (CTTT,1), (AGTA,13), (CCGT,1), (GTAA,8), (TCAA,11), (TCAC,5), (TGTT,5), (ACTC,5), (ATTT,7), (GGAT,1), (TTTG,6), (AAAT,8), (TCGA,5), (AATA,4), (ATCA,6), (CTCA,5), (AATC,5), (GGGT,1), (GCCT,3), (TCCC,1), (TCTT,1), (TGGA,1), (TCCA,6), (TAAG,3), (TTTA,1), (TAAA,4), (AAGT,3), (ATCG,5), (CAGT,6), (GATC,6), (AGTG,1), (CGAT,5), (GTGG,1), (ATCC,6), (AACT,5))
topN=Array((AGTA,13), (TCAA,11), (CAAA,8))
 */
