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
object KMERFasta {

  // ====================================================
  //  drop non DNA sequences
  def filterRecords(rec: String): Boolean = {
    if (rec == null) return false
    val strippedRec = rec.strip()
    if (strippedRec.length < 1) return false
    if (strippedRec.charAt(0) == '>') return false
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
records=Array(>SEQUENCE_1, GATTTGGGGCCCAAAGCAGTATCGATGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCAAATAGTGGATCCATTTGTTCAACTCACAGTTTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT, >SEQUENCE_2, GATTTGATTTGGGGCCCAAAGCAGTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTATCGATCAAATAGTGGATCGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCATTTGTTCAACTCACAGTTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT)
sequences=Array(GATTTGGGGCCCAAAGCAGTATCGATGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCAAATAGTGGATCCATTTGTTCAACTCACAGTTTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT, GATTTGATTTGGGGCCCAAAGCAGTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTATCGATCAAATAGTGGATCGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCATTTGTTCAACTCACAGTTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT)
pairs=Array((GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATG,1), (ATGA,1), (TGAT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (TTTC,1), (TTCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (TTTG,1), (TTGA,1), (TGAT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGA,1), (TGAT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTG,1), (GTGA,1), (TGAT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (TTTA,1), (TTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATT,1), (ATTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (TTTC,1), (TTCA,1), (TCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1), (TTTG,1), (TTGG,1), (TGGG,1), (GGGG,1), (GGGC,1), (GGCC,1), (GCCC,1), (CCCA,1), (CCAA,1), (CAAA,1), (AAAG,1), (AAGC,1), (AGCA,1), (GCAG,1), (CAGT,1), (AGTA,1), (GTAT,1), (TATC,1), (ATCG,1), (TCGA,1), (CGAT,1), (GATC,1), (ATCA,1), (TCAA,1), (CAAA,1), (AAAT,1), (AATA,1), (ATAG,1), (TAGT,1), (AGTG,1), (GTGG,1), (TGGA,1), (GGAT,1), (GATC,1), (ATCC,1), (TCCA,1), (CCAT,1), (CATT,1), (ATTT,1), (TTTG,1), (TTGT,1), (TGTT,1), (GTTC,1), (TTCA,1), (TCAA,1), (CAAC,1), (AACT,1), (ACTC,1), (CTCA,1), (TCAC,1), (CACA,1), (ACAG,1), (CAGT,1), (AGTT,1), (GTTT,1))
frequencies=Array((GGGC,7), (CAAC,7), (TCAT,1), (GCCC,7), (TTCA,9), (AGTT,7), (GATT,7), (GCAG,7), (TAGT,7), (TTGA,2), (GGCC,7), (AGCA,7), (CAAA,14), (GGGG,7), (ACAG,7), (CACA,7), (TGAT,4), (AAAG,7), (CATT,7), (CCAA,7), (CCCA,7), (AAGC,7), (TATC,7), (GTAT,6), (TTGG,7), (GTTC,7), (ATAG,7), (TGGG,7), (GATG,1), (TTGT,7), (TTTC,2), (GTTT,7), (CCAT,6), (GTGA,1), (AGTA,6), (ATGA,1), (TCAA,14), (TCAC,7), (TGTT,7), (ACTC,7), (ATTT,14), (GGAT,7), (TTTG,16), (AAAT,7), (TCGA,8), (AATA,7), (ATCA,6), (CTCA,7), (TGGA,7), (TTTA,1), (TCCA,6), (TTAT,1), (ATCG,8), (CAGT,14), (GATC,13), (AGTG,8), (CGAT,8), (GTGG,7), (ATCC,6), (AACT,7))
topN=Array((TTTG,16), (TCAA,14), (CAAA,14))
 */
