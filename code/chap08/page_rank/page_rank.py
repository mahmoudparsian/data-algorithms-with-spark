"""
This is an example implementation of PageRank. 
For more conventional use, please refer to the
PageRank implementation provided by graphframes 
and graphx.

Input Data Format:
    <source-URL-ID><,><neighbor-URL-ID>

Example Usage:

    export num_of_iterations=20
    export input_path="pagerank_data.txt"
    spark-submit pagerank.py $input_path $num_of_iterations
"""


from __future__ import print_function
import sys
from pyspark.sql import SparkSession

#---------------------------------------------
#
# For Debugging and testing purposes
# Do not use for production environment
# Collects all URL ranks and dump them to console.
def print_ranks(ranks):
    for (link, rank) in sorted(ranks.collect()):
        print("%s has rank: %s." % (link, round(rank,2)))
#end-def
#---------------------------------------------
#
def recalculate_rank(rank):
    new_rank = rank * 0.85 + 0.15
    return new_rank
#end-def
#---------------------------------------------
#
def compute_contributions(urls_rank):
    """Calculates URL contributions to the rank of other URLs."""
    urls = urls_rank[1][0]
    rank = urls_rank[1][1]
    #
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
#end-def
#---------------------------------------------
#
def create_pair(urls):
    """Parses a urls pair string into urls pair."""
    tokens = urls.split(",")
    source_URL = tokens[0]
    neighbor_URL = tokens[1]
    return (source_URL, neighbor_URL)
#end-def


# STEP-1: Read input parameters:
input_path = sys.argv[1]
num_of_iterations = int(sys.argv[2])


# STEP-2: Initialize the spark session.
spark = SparkSession.builder.getOrCreate()

# STEP-3: Create RDD[String] from input_path
records = spark.sparkContext.textFile(input_path)

# STEP-4: Create initial links
# Loads all URLs from input file and initialize their neighbors.
links = records.map(lambda rec: create_pair(rec)).distinct().groupByKey().cache()

# STEP-5: Initialize ranks to 1.0
# Loads all URLs with other URL(s) link to from
# input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# print("----")
# print(ranks.collect())
# print("----")

# STEP-6: Perform iterations...
# Calculates and updates URL ranks continuously using PageRank algorithm.
for iteration in range(num_of_iterations):
    # debug ranks
    print_ranks(ranks)
    
    # Calculates URL contributions to the rank of other URLs.
    contributions = links.join(ranks).flatMap(compute_contributions)

    # links.join(ranks) will create elements as:
    # [
    #  (u'1', (<pyspark.resultiterable.ResultIterable object at 0x10b5d1950>, 0.9203125)), 
    #  (u'3', (<pyspark.resultiterable.ResultIterable object at 0x10b6ad450>, 0.6334375)),
    # ...
    # ]

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contributions.reduceByKey(lambda x,y : x+y).mapValues(recalculate_rank)
#end-for

# STEP-7: Display the page ranks
# Collects all URL ranks and dump them to console.
print_ranks(ranks)

# STEP-8: done
spark.stop()
