#-----------------------------------------------------
# This is a DNA-Base-Count in PySpark.
# It uses mapPartitions() transformation
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
from collections import defaultdict
from pyspark.sql import SparkSession
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
# create a set of (dna-letter, 1) pairs for a partition
def process_fasta_per_partition(partition):
    # create an empty dictionary
    hashmap = defaultdict(int)
    #
    for element in partition:
        chars = element.upper()
        for c in chars: 
            if c in ("A", "T", "C", "G"): hashmap[c] += 1
    #end-for
    return [(k, v) for k, v in hashmap.items()]
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
    counts_per_partition = filtered.mapPartitions(process_fasta_per_partition)
    print("counts_per_partition.count() : ", counts_per_partition.count())
    print("counts_per_partition.collect() : ", counts_per_partition.collect())

    frequencies = counts_per_partition.reduceByKey(lambda x, y: x+y)
    print("frequencies.collect() : ", frequencies.collect())

    spark.stop()
#end-def
#-----------------------------------

if __name__ == "__main__":
    main()