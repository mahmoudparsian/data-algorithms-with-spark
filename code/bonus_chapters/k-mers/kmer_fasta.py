from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

"""
This PySpark code provides K-mer counting functionality.
  
Kmer counting for a given K and N, where
K: to find K-mers
N: to find top-N

Usage: $SPARK_HOME/bin/spark-submit kmer_fasta.py <fasta-input-path> <K> <N>

FASTA format: https://en.wikipedia.org/wiki/FASTA_format

A k-mer (or kmer) is a short DNA sequence consisting of 
a fixed number (K) of bases. The value of k is usually 
divisible by 4 so that a kmer can fit compactly into a 
basevector object. Typical values include 12, 20, 24, 36, 
and 48; kmers of these sizes are referred to as 12-mers, 
20-mers, and so forth.
 
NOTE: print() and collect() are used for debugging and educational purposes.
 
@author Mahmoud Parsian
 
"""

#----------------------------------------------------
# drop non DNA sequences
def filter_records(rec):
    if rec is None: return False
    stripped_rec = rec.strip()
    if len(stripped_rec) < 1: return False
    if stripped_rec[0] == ">": return False
    return True
#end-def
#----------------------------------------------------
# generate K-mers pairs: (k-mer, 1)
def generate_kmers(dna, KB):
    # print("dna=", dna)
    k = KB.value
    # print("k=", k)
    #
    if len(dna) < k: return []
    list_of_pairs = []
    for x in range(len(dna)+1-k):
        kmer = dna[x:x+k]
        list_of_pairs.append((kmer, 1))
    #end-for
    return list_of_pairs
#end-def
#----------------------------------------------------
def main():

    # define input parameters: FASTA format
    fasta_input_path =  sys.argv[1]
    K = int(sys.argv[2]) # to find K-mers
    N = int(sys.argv[3]) # to find top-N
    #
    print("K=", K)
    print("N=", N)
    print("fasta_input_path=", fasta_input_path)
    
    
    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    print("spark.version=", spark.version)
   
    # make K as a distributed data/value
    KB = sc.broadcast(K)
    print("KB.value=", KB.value)

    # read input and create the first RDD  as RDD[String] 
    records = sc.textFile(fasta_input_path)
    print("records=", records.collect())
    
    # drop records, which are not DNA sequences
    sequences = records.filter(filter_records)
    print("sequences=", sequences.collect())
      
    # generate K-mers pairs: (k-mer, 1)
    pairs = sequences.flatMap(lambda seq: generate_kmers(seq, KB))
    print("pairs=", pairs.collect())
    
    # find frequency of kmers
    frequencies = pairs.reduceByKey(lambda x, y: x+y)
    print("frequencies=", frequencies.collect())

    # emit final topN descending
    topN = frequencies.takeOrdered(N, key=lambda x: -x[1])
    print("topN=", topN)
    
    # done
    spark.stop()
#end-def
#----------------------------------------------------
if __name__ == '__main__':
    main()

"""
sample run:

$SPARK_HOME/bin/spark-submit kmer_fasta.py sample_1.fasta 4 3
K= 4
N= 3
fastq_input_path= sample_1.fasta
spark.version= 3.2.0
KB.value= 4

records= ['>SEQUENCE_1', 'GATTTGGGGCCCAAAGCAGTATCGATGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCAAATAGTGGATCCATTTGTTCAACTCACAGTTTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT', '>SEQUENCE_2', 'GATTTGATTTGGGGCCCAAAGCAGTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTATCGATCAAATAGTGGATCGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCATTTGTTCAACTCACAGTTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT']

sequences= ['>SEQUENCE_1', 'GATTTGGGGCCCAAAGCAGTATCGATGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCAAATAGTGGATCCATTTGTTCAACTCACAGTTTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT', '>SEQUENCE_2', 'GATTTGATTTGGGGCCCAAAGCAGTGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTATCGATCAAATAGTGGATCGATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTTCATTTGTTCAACTCACAGTTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT']

pairs= [('>SEQ', 1), ('SEQU', 1), ('EQUE', 1), ('QUEN', 1), ('UENC', 1), ('ENCE', 1), ('NCE_', 1), ('CE_1', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATG', 1), ('ATGA', 1), ('TGAT', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('TTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('TTTG', 1), ('TTGA', 1), ('TGAT', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('>SEQ', 1), ('SEQU', 1), ('EQUE', 1), ('QUEN', 1), ('UENC', 1), ('ENCE', 1), ('NCE_', 1), ('CE_2', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGA', 1), ('TGAT', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTG', 1), ('GTGA', 1), ('TGAT', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('TTTA', 1), ('TTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('TTTC', 1), ('TTCA', 1), ('TCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1)]

frequencies= [('QUEN', 2), ('ENCE', 2), ('NCE_', 2), ('TTTG', 16), ('TGGG', 7), ('GGGG', 7), ('GGCC', 7), ('GCCC', 7), ('AGTA', 6), ('CGAT', 8), ('TGAT', 4), ('TCAA', 14), ('AATA', 7), ('TAGT', 7), ('AGTG', 8), ('GGAT', 7), ('ATCC', 6), ('TCCA', 6), ('CCAT', 6), ('TTCA', 9), ('CAAC', 7), ('AACT', 7), ('ACTC', 7), ('CTCA', 7), ('CACA', 7), ('TTTC', 2), ('GTGA', 1), ('TTTA', 1), ('>SEQ', 2), ('SEQU', 2), ('EQUE', 2), ('UENC', 2), ('CE_1', 1), ('GATT', 7), ('ATTT', 14), ('TTGG', 7), ('GGGC', 7), ('CCCA', 7), ('CCAA', 7), ('CAAA', 14), ('AAAG', 7), ('AAGC', 7), ('AGCA', 7), ('GCAG', 7), ('CAGT', 14), ('GTAT', 6), ('TATC', 7), ('ATCG', 8), ('TCGA', 8), ('GATG', 1), ('ATGA', 1), ('GATC', 13), ('ATCA', 6), ('AAAT', 7), ('ATAG', 7), ('GTGG', 7), ('TGGA', 7), ('CATT', 7), ('TTGT', 7), ('TGTT', 7), ('GTTC', 7), ('TCAC', 7), ('ACAG', 7), ('AGTT', 7), ('GTTT', 7), ('TTGA', 2), ('CE_2', 1), ('TTAT', 1), ('TCAT', 1)]

topN= [('TTTG', 16), ('TCAA', 14), ('ATTT', 14)]

"""