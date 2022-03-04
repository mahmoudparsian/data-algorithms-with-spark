from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

"""
This PySpark code provides K-mer counting functionality.
  
Kmer counting for a given K and N, where
K: to find K-mers
N: to find top-N

Usage: $SPARK_HOME/bin/spark-submit kmer_fastq.py <fastq-input-path> <K> <N>

FASTQ file format: https://en.wikipedia.org/wiki/FASTQ_format

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
    if stripped_rec[0] in ("@", "+", ";", "!", "~"): return False
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

    # define input parameters
    fastq_input_path =  sys.argv[1]
    K = int(sys.argv[2]) # to find K-mers
    N = int(sys.argv[3]) # to find top-N
    #
    print("K=", K)
    print("N=", N)
    print("fastq_input_path=", fastq_input_path)
    
    
    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    print("spark.version=", spark.version)
   
    # make K as a distributed data/value
    KB = sc.broadcast(K)
    print("KB.value=", KB.value)

    # read input and create the first RDD  as RDD[String] 
    records = sc.textFile(fastq_input_path)
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

$SPARK_HOME/bin/spark-submit kmer_fastq.py sample_1.fastq 4 3
K= 4
N= 3
fastq_input_path= sample_1.fastq
spark.version= 3.2.0
KB.value= 4

records= ['@SEQ_ID', 'GATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT', '+', "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65", '@SEQ_ID', 'GATTTCCCGTTCAAAGCAGTATCGATCTTTTAGTAAATCCATTTGTTCAACTCACAGTTG', '+', "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65", '@SEQ_ID', 'GACCCGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT', '+', "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65", '@SEQ_ID', 'TCATCATCATCCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT', '+', "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65", '@SEQ_ID', 'AGTAAGTAAGTAATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCTAGTAAGTA', '+', "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65"]

sequences= ['GATTTGGGGCCCAAAGCAGTATCGATCAAATAGTGGATCCATTTGTTCAACTCACAGTTT', 'GATTTCCCGTTCAAAGCAGTATCGATCTTTTAGTAAATCCATTTGTTCAACTCACAGTTG', 'GACCCGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT', 'TCATCATCATCCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCT', 'AGTAAGTAAGTAATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGCCTAGTAAGTA']

pairs= [('GATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGG', 1), ('TGGG', 1), ('GGGG', 1), ('GGGC', 1), ('GGCC', 1), ('GCCC', 1), ('CCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTG', 1), ('GTGG', 1), ('TGGA', 1), ('GGAT', 1), ('GATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTT', 1), ('GATT', 1), ('ATTT', 1), ('TTTC', 1), ('TTCC', 1), ('TCCC', 1), ('CCCG', 1), ('CCGT', 1), ('CGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCT', 1), ('TCTT', 1), ('CTTT', 1), ('TTTT', 1), ('TTTA', 1), ('TTAG', 1), ('TAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAA', 1), ('AAAT', 1), ('AATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGT', 1), ('AGTT', 1), ('GTTG', 1), ('GACC', 1), ('ACCC', 1), ('CCCG', 1), ('CCGG', 1), ('CGGG', 1), ('GGGG', 1), ('GGGT', 1), ('GGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAA', 1), ('AAAT', 1), ('AATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGC', 1), ('AGCC', 1), ('GCCT', 1), ('TCAT', 1), ('CATC', 1), ('ATCA', 1), ('TCAT', 1), ('CATC', 1), ('ATCA', 1), ('TCAT', 1), ('CATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAA', 1), ('CAAA', 1), ('AAAG', 1), ('AAGC', 1), ('AGCA', 1), ('GCAG', 1), ('CAGT', 1), ('AGTA', 1), ('GTAT', 1), ('TATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAA', 1), ('AAAT', 1), ('AATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGC', 1), ('AGCC', 1), ('GCCT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAG', 1), ('AAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAG', 1), ('AAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAT', 1), ('AATC', 1), ('ATCG', 1), ('TCGA', 1), ('CGAT', 1), ('GATC', 1), ('ATCA', 1), ('TCAA', 1), ('CAAA', 1), ('AAAT', 1), ('AATA', 1), ('ATAG', 1), ('TAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAA', 1), ('AAAT', 1), ('AATC', 1), ('ATCC', 1), ('TCCA', 1), ('CCAT', 1), ('CATT', 1), ('ATTT', 1), ('TTTG', 1), ('TTGT', 1), ('TGTT', 1), ('GTTC', 1), ('TTCA', 1), ('TCAA', 1), ('CAAC', 1), ('AACT', 1), ('ACTC', 1), ('CTCA', 1), ('TCAC', 1), ('CACA', 1), ('ACAG', 1), ('CAGC', 1), ('AGCC', 1), ('GCCT', 1), ('CCTA', 1), ('CTAG', 1), ('TAGT', 1), ('AGTA', 1), ('GTAA', 1), ('TAAG', 1), ('AAGT', 1), ('AGTA', 1)]

frequencies= [('TTTG', 6), ('TGGG', 1), ('GGGG', 2), ('GGCC', 1), ('GCCC', 1), ('AGTA', 13), ('CGAT', 5), ('TCAA', 11), ('AATA', 4), ('TAGT', 6), ('AGTG', 1), ('GGAT', 1), ('ATCC', 6), ('TCCA', 6), ('CCAT', 5), ('TTCA', 7), ('CAAC', 5), ('AACT', 5), ('ACTC', 5), ('CTCA', 5), ('CACA', 5), ('TTTC', 1), ('TTCC', 1), ('TCCC', 1), ('CCGT', 1), ('CTTT', 1), ('TTTA', 1), ('TTAG', 1), ('GTAA', 8), ('TAAA', 4), ('ACCC', 1), ('CCGG', 1), ('GGGT', 1), ('GCCT', 3), ('CATC', 3), ('TAAG', 3), ('AAGT', 3), ('CCTA', 1), ('GATT', 2), ('ATTT', 7), ('TTGG', 1), ('GGGC', 1), ('CCCA', 1), ('CCAA', 2), ('CAAA', 8), ('AAAG', 4), ('AAGC', 4), ('AGCA', 4), ('GCAG', 4), ('CAGT', 6), ('GTAT', 4), ('TATC', 4), ('ATCG', 5), ('TCGA', 5), ('GATC', 6), ('ATCA', 6), ('AAAT', 8), ('ATAG', 4), ('GTGG', 1), ('TGGA', 1), ('CATT', 5), ('TTGT', 5), ('TGTT', 5), ('GTTC', 7), ('TCAC', 5), ('ACAG', 5), ('AGTT', 2), ('GTTT', 1), ('CCCG', 2), ('CGTT', 1), ('ATCT', 1), ('TCTT', 1), ('TTTT', 1), ('AATC', 5), ('GTTG', 1), ('GACC', 1), ('CGGG', 1), ('GGTT', 1), ('CAGC', 3), ('AGCC', 3), ('TCAT', 3), ('TAAT', 1), ('CTAG', 1)]

topN= [('AGTA', 13), ('TCAA', 11), ('GTAA', 8)]


"""