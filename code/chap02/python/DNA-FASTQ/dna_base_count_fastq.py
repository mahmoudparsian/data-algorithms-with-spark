#!/usr/bin/python
#-----------------------------------------------------
# Version-3
# This is a DNA-Base-Count in PySpark using FASQT input data.
# The goal is to show how "DNA-Base-Count" works.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path for FASTQ data
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from collections import defaultdict
from pyspark import StorageLevel
#
#-----------------------------------
# rec: a single record of FASTQ file
#
# drop Lines 1, 3, and 4
# keep only Line2, which is a DNA sequence
def drop_3_records(rec):
  first_char = rec[0]
  # drop Line 1
  if first_char == '@':
    return False
  # drop Line 3
  if first_char == '+':
    return False 
  #
  # drop Line 4 & keep Line 2
  non_dna_letters = set('-+*/<>=.:@?;0123456789bde')
  if any((c in non_dna_letters) for c in rec):
    # drop Line 4
    print("rec=", rec)
    return False
  else:
    # found Line 2: DNA Sequence
    return True
#end-def    
#-----------------------------------
#
# we get an iterator: which represents
# a single partition of source RDD
#
# This function creates a hash map of DNA Letters
# "z" denotes the total number of FASTQ Records 
# for a single partition
#
def process_FASTQ_partition(iterator):
  # create an empty dictionary
  hashmap = defaultdict(int)

  for fastq_record in iterator:
    hashmap['z'] += 1
    chars = fastq_record.lower()
    for c in chars:
      hashmap[c] += 1
    #end-for
  #end-for
  #print("hashmap=", hashmap)
  #
  # Python 2.x
  #key_value_list = [(k, v) for k, v in hashmap.iteritems()]

  # Python 3.x
  key_value_list = [(k, v) for k, v in hashmap.items()]

  #print("key_value_list=", key_value_list)
  return  key_value_list
#
#end-def
#-----------------------------------


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ", __file__, " <input-path>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession object
    spark = SparkSession\
        .builder\
        .appName("dna_base_count_fastq")\
        .getOrCreate()

    input_path = sys.argv[1]
    print("input_path : ", input_path)

    records_rdd = spark.sparkContext.textFile(input_path)\
                       .map(lambda rec: rec.lower())
    print("records_rdd : ", records_rdd.collect())
    #    
    # for every seq (as 4 lines), drop 3 lines
    dna_seqs = records_rdd.filter(drop_3_records)
    print("dna_seqs : ", dna_seqs.collect())

    # if you do not have enough RAM, then do the following
    # MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
    #records_rdd.persist(StorageLevel(True, True, False, False, 1))

    pairs_rdd = dna_seqs.mapPartitions(process_FASTQ_partition)
    print("pairs_rdd : debug")
    pairs_as_list = pairs_rdd.collect()
    print("pairs_as_list : ", pairs_as_list)

    frequencies_rdd = pairs_rdd.reduceByKey(lambda x, y: x+y)
    print("frequencies_rdd : debug")
    frequencies_as_list = frequencies_rdd.collect()
    print("frequencies_as_list : ", frequencies_as_list)

    spark.stop()
