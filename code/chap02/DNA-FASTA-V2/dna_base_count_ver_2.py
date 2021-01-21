#!/usr/bin/python
#-----------------------------------------------------
# Version-2
# This is a DNA-Base-Count in PySpark.
# The goal is to show how "DNA-Base-Count" works.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from collections import defaultdict
from pyspark import StorageLevel

#-----------------------------------
#
# fasta_record: we get a single FASTA record as an argument
# create a hash map of DNA Letters
#
def process_FASTA_as_hashmap(fasta_record):
    if (fasta_record.startswith(">")):
        return [("z", 1)]
    #
    hashmap = defaultdict(int)
    chars = fasta_record.lower()
    #
    for c in chars:
        hashmap[c] += 1
#   end-for
    #print("hashmap=", hashmap)
#
    # Python 2.x
    key_value_list = [(k, v) for k, v in hashmap.iteritems()]

    # Python 3.x
    #key_value_list = [(k, v) for k, v in hashmap.items()]

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
        .appName("dna_base_count_ver_2")\
        .getOrCreate()

    input_path = sys.argv[1]
    print("input_path : ", input_path)

    #recordsRDD = spark.read.text(inputPath).rdd.map(lambda r: r[0])
    recordsRDD = spark.sparkContext.textFile(input_path)
    #print("recordsRDD.count() : ", recordsRDD.count())
    #recordsAsList = recordsRDD.collect()
    #print("recordsAsList : ", recordsAsList)

    # if you do not have enough RAM, then do the following
    # MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
    #recordsRDD.persist(StorageLevel(True, True, False, False, 1))

    pairsRDD = recordsRDD.flatMap(lambda rec: process_FASTA_as_hashmap(rec))
    #pairsRDD = recordsRDD.flatMap(processFastaAsMap)
    #print("pairsRDD : debug")
    #pairsAsList = pairsRDD.collect()
    #print("pairsAsList : ", pairsAsList)

    frequenciesRDD = pairsRDD.reduceByKey(lambda x, y: x+y)
    print("frequenciesRDD : debug")
    frequenciesAsList = frequenciesRDD.collect()
    print("frequenciesAsList : ", frequenciesAsList)

    spark.stop()
