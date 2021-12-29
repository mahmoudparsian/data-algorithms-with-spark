from __future__ import print_function
from pyspark.sql import SparkSession
import sys
import nltk

#-----------------------------------------------------
# flatMap() is a 1-to-Many transformation:
# each source element can be mapped to zero, 
# 1, 2, 3, ... , or more elements
#
# Apply a flatMap() transformation to an RDD
# Input: NONE
#
# print() is used for educational purposes.
#------------------------------------------------------
# Input Parameters:
#    file
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
# Find Frequency of Bigrams:
#
# In this example, we read a file and find 
# frequency of bigrams. If a record is empty
# or has a single word, then that record will 
# be dropped.
#
# You may install nltk package as:
# pip3 install --user -U nltk
#
# How to use nltk.bigrams:
# >>> import nltk
# >>> text = "this is a sentence one"
# >>> tokens = text.split(" ")
# >>> tokens
# ['this', 'is', 'a', 'sentence', 'one']
# >>> bigrams = nltk.bigrams(tokens)
# >>> bigrams
# <generator object bigrams at 0x10bef3b30>
#
# >>> list(bgs)
# [('this', 'is'), ('is', 'a'), ('a', 'sentence'), ('sentence', 'one')]

#=========================================
# rec = an element of source RDD = "word1 word2 word3 word4"
# create bigrams as a list [(word1, word2), (word2, word3), (word3, word4)]
def create_bigrams(rec):
    if rec is None: return []
    rec_stripped = rec.strip()
    if (len(rec_stripped) < 1): return []
    # assume that words are separated by a space
    tokens = rec_stripped.split(" ")
    if (len(tokens) < 2): return []
    #
    return nltk.bigrams(tokens)
#end-def
#==========================================

def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()

    #========================================
    # flatMap() transformation
    #
    # source_rdd.flatMap(function) --> target_rdd
    #
    # flatMap() is a 1-to-Many transformation
    #
    # PySpark's flatMap() is a transformation operation that flattens 
    # the RDD/DataFrame (array/map DataFrame columns) after applying the 
    # function on every element and returns a new PySpark RDD/DataFrame.
    #
    #========================================
    
    # define input path
    input_path = sys.argv[1]
    print("input_path = ", input_path)

    # create rdd : RDD[String]
    rdd = spark.sparkContext.textFile(input_path)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    #------------------------------------
    # apply a flatMap() transformation to rdd
    #------------------------------------
    # rdd2 : RDD[[(word1, word2)]]
    bigrams = rdd.flatMap(create_bigrams)
    print("bigrams = ", bigrams)
    print("bigrams.count() = ", bigrams.count())
    print("bigrams.collect() = ", bigrams.collect())


    #------------------------------------
    # find frequency of bigrams
    #------------------------------------
    # rdd3 : create (bigram, 1) pairs
    key_value = bigrams.map(lambda b : (b, 1))
    print("key_value = ", key_value)
    print("key_value.count() = ", key_value.count())
    print("key_value.collect() = ", key_value.collect())
    #
    frequency = key_value.reduceByKey(lambda x, y: x+y)
    print("frequency = ", frequency)
    print("frequency.count() = ", frequency.count())
    print("frequency.collect() = ", frequency.collect())

    # done!
    spark.stop()
#end-def
#==========================================

if __name__ == '__main__':
    main()
    
"""
$SPARK_HOME/bin/spark-submit flatmap_transformation_1_from_file.py bigrams_input.txt       
input_path =  bigrams_input.txt
rdd =  bigrams_input.txt MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
rdd.count() =  6
rdd.collect() =  ['Spark shines in data analytics and beyond', 'this is the', 'this is the first record', 'Spark shines in data analytics and beyond', 'this is the second record', 'Spark shines again in data analytics and beyond']

bigrams =  PythonRDD[3] at RDD at PythonRDD.scala:53
bigrams.count() =  29
bigrams.collect() =  [('Spark', 'shines'), ('shines', 'in'), ('in', 'data'), ('data', 'analytics'), ('analytics', 'and'), ('and', 'beyond'), ('this', 'is'), ('is', 'the'), ('this', 'is'), ('is', 'the'), ('the', 'first'), ('first', 'record'), ('Spark', 'shines'), ('shines', 'in'), ('in', 'data'), ('data', 'analytics'), ('analytics', 'and'), ('and', 'beyond'), ('this', 'is'), ('is', 'the'), ('the', 'second'), ('second', 'record'), ('Spark', 'shines'), ('shines', 'again'), ('again', 'in'), ('in', 'data'), ('data', 'analytics'), ('analytics', 'and'), ('and', 'beyond')]

key_value =  PythonRDD[5] at RDD at PythonRDD.scala:53
key_value.count() =  29
key_value.collect() =  [(('Spark', 'shines'), 1), (('shines', 'in'), 1), (('in', 'data'), 1), (('data', 'analytics'), 1), (('analytics', 'and'), 1), (('and', 'beyond'), 1), (('this', 'is'), 1), (('is', 'the'), 1), (('this', 'is'), 1), (('is', 'the'), 1), (('the', 'first'), 1), (('first', 'record'), 1), (('Spark', 'shines'), 1), (('shines', 'in'), 1), (('in', 'data'), 1), (('data', 'analytics'), 1), (('analytics', 'and'), 1), (('and', 'beyond'), 1), (('this', 'is'), 1), (('is', 'the'), 1), (('the', 'second'), 1), (('second', 'record'), 1), (('Spark', 'shines'), 1), (('shines', 'again'), 1), (('again', 'in'), 1), (('in', 'data'), 1), (('data', 'analytics'), 1), (('analytics', 'and'), 1), (('and', 'beyond'), 1)]

frequency =  PythonRDD[11] at RDD at PythonRDD.scala:53
frequency.count() =  14
frequency.collect() =  [(('Spark', 'shines'), 3), (('shines', 'in'), 2), (('this', 'is'), 3), (('the', 'first'), 1), (('the', 'second'), 1), (('shines', 'again'), 1), (('again', 'in'), 1), (('in', 'data'), 3), (('data', 'analytics'), 3), (('analytics', 'and'), 3), (('and', 'beyond'), 3), (('is', 'the'), 3), (('first', 'record'), 1), (('second', 'record'), 1)]

"""