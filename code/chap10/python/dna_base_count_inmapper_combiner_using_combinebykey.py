#-----------------------------------------------------
# This is a DNA-Base-Count in PySpark.
# It uses InMapper-Combiner with combineByKey()
#------------------------------------------------------
# NOTE: print() and collect() are used for 
#       debugging and educational purposes.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark import StorageLevel
#-----------------------------------
#
# drop FASTA's commented records
def drop_commented_record(fasta_record):
	#
    if fasta_record is None: return False
    #
    stripped = fasta_record.strip()
    #
    if (len(fasta_record) < 2) or (stripped.startswith(">")): 
        return False
    else:
        # then it is a DNA string
        return True
    #end-if
#end-def
#-----------------------------------
# create a set of (dna-letter, 1) pairs
def inmapper_combiner(fasta_record):
    key_value_list = []
    #
    A, T, C, G = 0, 0, 0, 0
    #
    chars = fasta_record.upper()
    for c in chars:
        if c == "A": A +=1
        elif c == "T": T +=1
        elif c == "C": C +=1
        elif c == "G": G +=1
    #end-for
    key_value_list.append(("A", A))
    key_value_list.append(("T", T))
    key_value_list.append(("C", C))
    key_value_list.append(("G", G))    
    #
    return key_value_list
#end-def
#-----------------------------------
def main():
    #
    input_path = sys.argv[1]
    print("inputPath : ", input_path)
    
    # create an instance of SparkSession object
    spark = SparkSession.builder.getOrCreate()

    # records: RDD[String]
    records = spark.sparkContext.textFile(input_path)
    print("records.count() : ", records.count())
    
    # drop non-needed records 
    # filtered: RDD[String]
    filtered = records.filter(drop_commented_record)
    print("filtered.count() : ", filtered.count())

    # pairs: RDD[(String, Integer)]
    pairs = filtered.flatMap(inmapper_combiner)
    print("pairs.count() : ", pairs.count())
    print("pairs.take(3) : ", pairs.take(3))


    frequencies = pairs.combineByKey(\
        lambda v: v, \
        lambda C, v: C+v, \
        lambda C, D: C+D)
    print("frequencies.collect() : ", frequencies.collect())

    spark.stop()
#end-def
#-----------------------------------

if __name__ == "__main__":
    main()