#!/usr/bin/python
#-----------------------------------------------------
# Version-1
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
from pyspark import StorageLevel
#-----------------------------------
# "z" denotes the total number of FASTA Records
def process_FASTA_record(fasta_record):
    key_value_list = []
#
    if (fasta_record.startswith(">")):
        key_value_list.append(("z", 1))
    else:
        chars = fasta_record.lower()
        #print("chars=", chars)
        for c in chars:
            #print("c=", c)
            key_value_list.append((str(c), 1))
        #end-for
    #end-if
    #print("key_value_list=", key_value_list)
    return key_value_list
#
#end-def
#-----------------------------------

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ", __file__, "  <input-path>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession object
    spark = SparkSession\
        .builder\
        .appName("dna_base_count_ver_1")\
        .getOrCreate()

    input_path = sys.argv[1]
    print("inputPath : ", input_path)

    #recordsRDD = spark.read.text(inputPath).rdd.map(lambda r: r[0])
    recordsRDD = spark.sparkContext.textFile(input_path)
    print("recordsRDD.count() : ", recordsRDD.count())
    #recordsAsList = recordsRDD.collect()
    #print("recordsAsList : ", recordsAsList)

    # if you do not have enough RAM, then do the following
    # MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
    #recordsRDD.persist(StorageLevel(True, True, False, False, 1))
    #
    pairsRDD = recordsRDD.flatMap(lambda rec: process_FASTA_record(rec))
    #pairsRDD = recordsRDD.flatMap(processFASTARecord)
    #print("pairsRDD : debug")
    #recordsAsList = pairsRDD.collect()
    #print("recordsAsList : ", recordsAsList)

    frequenciesRDD = pairsRDD.reduceByKey(lambda x, y: x+y)
    print("frequenciesRDD : debug")
    frequenciesAsList = frequenciesRDD.collect()
    print("frequenciesAsList : ", frequenciesAsList)

    spark.stop()
