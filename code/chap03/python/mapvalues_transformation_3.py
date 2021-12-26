from __future__ import print_function
from pyspark.sql import SparkSession
from collections import defaultdict
from collections import Counter


#-----------------------------------------------------
# RDD.mapValues(f)
# Pass each value in the key-value pair RDD through 
# a map function without changing the keys; this also 
# retains the original RDD’s partitioning.
#
# Note that mapValues() can be accomplished by map().
#
# Let rdd = RDD[(K, V)]
# then the following RDDs (rdd2, rdd3) are equivalent:
#
#    rdd2 = rdd.mapValues(f)
#
#    rdd3 = rdd.map(lambda x: (x[0], f(x[1])))
#
# print() is used for educational purposes.
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
#
# Count DNA bases
# version 1: simple version with default dictionary
#
def count_DNA_bases_1(dna_seq):
    counter = defaultdict(int)
    for letter in dna_seq:
        counter[letter] += 1
    #end-for
    return counter
#end-def
#-------------------------------------------------------
#
# Count DNA bases
# version 2: simplified function by using Counter
#
def count_DNA_bases(dna_seq):
    return Counter(dna_seq)
#end-def
#-------------------------------------------------------

def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a list of (K, V)
    list_of_key_value= [ ("seq1", "AATCGGCCAAAGG"), \
                         ("seq2", "ATTATCGGCCAAAGCTTTCG"), \
                         ("seq1", "AAATTATCGGCCAAAGG") ]
    print("list_of_key_value = ", list_of_key_value)
    
    # create rdd : RDD[(String, String)]
    # key: String
    # value = [Integer]
    rdd = spark.sparkContext.parallelize(list_of_key_value)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    #------------------------------------
    # apply a mapValues() transformation to rdd
    #------------------------------------
    # DNA Base Count: find count of A's, T's, C's, G's
    # rdd_mapped : RDD[(String, dictionary)]
    #  where dictionary is a hash table with keys {A, T, C, G}
    #
    dna_base_count = rdd.mapValues(count_DNA_bases)
    print("dna_base_count = ", dna_base_count)
    print("dna_base_count.count() = ", dna_base_count.count())
    print("dna_base_count.collect() = ", dna_base_count.collect())


    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()

"""
sample run:

export Spark_HOME=/book/spark-3.2.0     
$SPARK_HOME/bin/spark-submit mapvalues_transformation_3.py    

list_of_key_value =  
[
 ('seq1', 'AATCGGCCAAAGG'), 
 ('seq2', 'ATTATCGGCCAAAGCTTTCG'), 
 ('seq1', 'AAATTATCGGCCAAAGG')
]

rdd =  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
rdd.count() =  3
rdd.collect() =  
[
 ('seq1', 'AATCGGCCAAAGG'), 
 ('seq2', 'ATTATCGGCCAAAGCTTTCG'), 
 ('seq1', 'AAATTATCGGCCAAAGG')
]

dna_base_count =  PythonRDD[2] at RDD at PythonRDD.scala:53
dna_base_count.count() =  3
dna_base_count.collect() =  
[
 ('seq1', Counter({'A': 5, 'G': 4, 'C': 3, 'T': 1})), 
 ('seq2', Counter({'T': 6, 'A': 5, 'C': 5, 'G': 4})), 
 ('seq1', Counter({'A': 7, 'G': 4, 'T': 3, 'C': 3}))
]

"""