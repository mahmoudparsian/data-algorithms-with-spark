from __future__ import print_function
import sys
import re
from pyspark.sql import SparkSession
from collections import Counter

"""
 * Find anagram counts for a given set of documents.
 * For example, if the sample input is comprised of 
 * the following 3 lines:
 * 
 *     Mary and Elvis lives in Detroit army Easter Listen 
 *     silent eaters Death Hated elvis Mary easter Silent
 *     Mary and Elvis are in army Listen Silent detroit
 * 
 * Then the output will be:
 *     
 *     Sorted word      Anagrams and Frequencies
 *     ===========   -> ========================
 *     (adeht        -> {death=1, hated=1})
 *     (eilnst       -> {silent=3, listen=2})
 *     (eilsv        -> {lives=1, elvis=3})
 *     (aeerst       -> {eaters=1, easter=2})
 *     (amry         -> {army=2, mary=3})
  * 
 * Since "in", "and", "are", "detroit" don't have 
 * an associated anagrams, they will be filtered 
 * out (dropped out):
 * 
 *     in -> null
 *     are -> null
 *     and -> null
 *     Detroit -> null
 *
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *
 * @author Mahmoud Parsian
 *
"""
#---------------------------------------
def to_words(x):
    x = re.sub(r"[^0-9A-Za-z]+", " ", x)
    x = re.sub(r"[ ]+", " ", x)
    return x.lower().split()
#end-def
#---------------------------------------
def sort_chars(w):
    return ("".join(sorted(w)), [w])
#end-def
#---------------------------------------
def sort_word(word):
    sorted_characters = sorted(word)
    sorted_word = "".join(sorted_characters)
    return sorted_word
#end-def
#---------------------------------------
def unique_words(x):
    u = list(set(x[0]))
    return (len(u), u)
#end-def
#---------------------------------------    
def list_to_dict(L):
    return Counter(L)
#end-def
#---------------------------------------
def filter_redundant(hashtable):
    if (len(hashtable) > 1):
        return True
    else:
        return False
    #end-if
#end-def
#--------------------------------------- 
#---------------------------------------          
# main program:
#
input_path = sys.argv[1]
print("input_path=", input_path)

# if a word.length < word_length_threshold, that word will be ignored
#     word_length_threshold = int(sys.argv[2])
#     print("word_length_threshold=", word_length_threshold)

# create an instance of a SparkSession as spark
spark = SparkSession.builder.getOrCreate()

# create an RDD[String] for input
records = spark.sparkContext.textFile(input_path);
print("records=", records.collect())

# create RDD[(K, V)] from input
# where K = sorted(word) and V = word
rdd = records.flatMap(to_words)
print("rdd=", rdd.collect())

rdd_key_value = rdd.map(lambda x: (sort_word(x), x))
        
# create anagrams as: RDD[(K, Iterable<String>)]
anagrams_list = rdd_key_value.groupByKey()
print("anagrams_list=", anagrams_list.mapValues(lambda l: list(l)).collect())

# anagrams: RDD[(K, Map<word, Integer>)]
anagrams = anagrams_list.mapValues(list_to_dict)
print("anagrams=", anagrams.collect())

# filter out the redundant RDD elements: 
# now we should filter (k,v) pairs from anagrams RDD:
# where k is a "sorted word" and v is a Map<String,Integer>
# if len(v) == 1 then it means that there is no associated
# anagram for the diven "sorted word".
#
# For example our anagrams will have the following RDD entry:
# (k=Detroit, v=Map.Entry("detroit", 2))
# since the size of v (i.e., the hash map) is one that will 
# be dropped out
#
# x: 
#    x[0]: sorted_word
#    x[1]: Map<word, Integer>
filtered_anagrams = anagrams.filter(lambda x: filter_redundant(x[1]))
print("filtered_anagrams=", filtered_anagrams.collect())
