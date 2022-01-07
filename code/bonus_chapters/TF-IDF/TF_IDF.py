from __future__ import print_function
#
from pyspark.sql import SparkSession
#
import math
import sys

"""
TF-IDF is calculated using the term frequency (TF)
and inverse document frequency (IDF).

TF-IDF(w) = F * log ( N / M)

where
     w: is a single unique word
     F: represents the frequency of a word in a document
     N: total number of documents
     M: number of documents containing the word w

Computing Term Frequency (TF): TF is the number of times 
a particular word appears in a single document.

Sample data set: contains 4 documents:
   .../book/code/sampe_chapters/TF-IDF/data/doc1
   .../book/code/sampe_chapters/TF-IDF/data/doc2
   .../book/code/sampe_chapters/TF-IDF/data/doc3
   .../book/code/sampe_chapters/TF-IDF/data/doc4

Therefore, documents are: {doc1, doc2, doc3, doc4}
So, document IDs are: {doc1, doc2, doc3, doc4}
  
Computing Inverse Document Frequency (IDF)
The IDF score indicates the importance of a 
particular word in the whole set of documents. 
For example, if a certain word is appearing in 
every document then the IDF score will be zero for 
that word.

IDF(w) = log ( N / M)


Algorithm: The TF-IDF is accomplished in 5 simple steps:
  Step-1: prepare input
  Step-2: calculate TF
  Step-3: calculate IDF
  Step-4: prepare for TF-IDF
  Step-5: prepare final output of TF-IDF  as a DataFrame    

NOTE: print() and collect() are used for debugging and educational purposes.

@author: Mahmoud Parsian
     
"""
#-----------------------------------------------------
# full_path = file:/book/code/bonus_chapters/TF-IDF/data/doc1
def keep_doc_name_and_clean(tuple2):
    full_path = tuple2[0]
    tokens = full_path.split("/")
    # get the last element of a list of doc path tokens
    short_doc_name = tokens[-1]
    # short_doc_name : "doc1"
    #
    # replace \n by " "
    document = tuple2[1]
    document_revised = document.replace('\n', ' ').strip()
    return (short_doc_name, document_revised)
#end-def
#-----------------------------------------------------
def create_key_value_pair(doc):
    docID = doc[0]
    document_content = doc[1]
    tokens = document_content.split()
    filtered_tokens = filter(lambda word: len(word) > word_length_threshold, tokens)
    return [((docID, w), 1) for w in filtered_tokens]
#end-def
#-----------------------------------------------------
#
# main() function:
#
# create an instance of a SparkSession as spark
spark = SparkSession.builder.getOrCreate()

# define input path
input_path = sys.argv[1]
print("input_path=", input_path)

# define word length threshold
# drop a word if its length is less than or equal to word_length_threshold
# drop words like "a", "of", ...
word_length_threshold = int(sys.argv[2])
print("word_length_threshold=", word_length_threshold)

#----------------------
# Step-1: prepare input
#----------------------
# read docs and create docs as
# docs : RDD[(key, value)] 
# where 
#      key: document_path
#      value: content of document
docs = spark.sparkContext.wholeTextFiles(input_path)
num_of_docs = docs.count()
print("doc=", docs.collect())


# keep only the name of the file (drop full path)
lines = docs.map(keep_doc_name_and_clean)
print("lines=", lines.collect())

#---------------------
# Step-2: calculate TF
#---------------------
# Find a frequency of a word within a document:
# So we need to find: ((docID, word), frequency)
# where key is (docID, word) and value is frequency.
# Get the term frequency for a particular word 
# corresponding to its docID.

# mapped = [((docID, word), 1)]
mapped = lines.flatMap(create_key_value_pair)
# reduced= [((docID, word), freq)]
reduced = mapped.reduceByKey(lambda x,y: x+y)
print("reduced=", reduced.collect())

# tf = [(word, (docID, freq))]
tf = reduced.map(lambda x: (x[0][1], (x[0][0], x[1])))
print("tf=", tf.collect())

# find frequency of unique words in all of the documents
words = reduced.map(lambda x: (x[0][1], 1))
# words = [(word, 1)]
print("words=", words.collect())

frequencies = words.reduceByKey(lambda x,y: x+y)
print("frequencies=", frequencies.collect())
# frequencies = [(word, freq)]

#----------------------
# Step-3: calculate IDF
#----------------------
idf = frequencies.map(lambda x: (x[0], math.log10(num_of_docs/x[1])))
print("idf=", idf.collect())

#---------------------------
# Step-4: prepare for TF-IDF
#---------------------------
tf_idf = tf.join(idf)\
           .map(lambda x: (x[1][0][0],(x[0], x[1][0][1], x[1][1], x[1][0][1]*x[1][1])))\
           .sortByKey()
print("tf_idf=", tf_idf.collect())

#-------------------------------------------------------
# Step-5: prepare final output of TF-IDF  as a DataFrame        
#-------------------------------------------------------
tf_idf_mapped = tf_idf.map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3]))
df = tf_idf_mapped.toDF(["DocID", "Token", "TF", "IDF", "TF-IDF"])
df.show(100, truncate=False)
                                                                                                                                           