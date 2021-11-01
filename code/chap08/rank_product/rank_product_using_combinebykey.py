from __future__ import print_function
import sys
from pyspark.sql import SparkSession

# rank_product_using_combinebykey.py
#
# NOTE: combineByKey() is used for grouping 
#       keys by their associated values.
# 
# Handles multiple studies, where each study is 
# a set of assays.  For each study, we find the 
# mean per gene and then calculate the rank product 
# for all genes.
#
# @author Mahmoud Parsian

#------------------------------------------
# rec = "gene_id,test_expression"
# return a pair(gene_id,test_expression)
def create_pair(rec):
    tokens = rec.split(",")
    gene_id = tokens[0]
    gene_value = float(tokens[1])
    return (gene_id, gene_value)
#end-def
#-------------------------------------------
# Compute mean per gene for a single study = set of assays
# @param input_Path set of assay paths separated by ","
# @RETURN RDD[(String, Double)]
def compute_mean(input_path):

   print("input_path", input_path)
   
   # genes as string records: RDD[String]
   raw_genes = spark.sparkContext.textFile(input_path)
   print("raw_genes", raw_genes.collect())
   
   # create RDD[(String, Double)]=RDD[(gene_id, test_expression)]
   genes = raw_genes.map(create_pair)  
   print("genes", genes.collect())
   
   # create RDD[(gene_id, (sum, count))]
   genes_combined = genes.combineByKey(
       # createCombiner, 
       lambda v: (v, 1),
       
       # addAndCount, 
       lambda C, v: (C[0]+v, C[1]+1),
       
       #mergeCombiners
       lambda C, D: (C[0]+D[0], C[1]+D[1])
   )
        
   print("genes_combined", genes_combined.collect())
   #now compute the mean per gene
   genes_mean = genes_combined.mapValues(lambda p: float(p[0])/float(p[1]))

   return genes_mean
#end-def       
#-------------------------------------------
# return RDD[(String, (Double, Integer))] = (gene_id, (ranked_product, N))
# where N is the number of elements for computing the ranked product
# @param ranks: RDD[(String, Long)][]
def compute_ranked_products(ranks):
    # combine all ranks into one  
    union_rdd = spark.sparkContext.union(ranks)
        
    # next find unique keys, with their associated copa scores
    # we need 3 function to be able to use combinebyKey()
    # combined_by_gene: RDD[(String, (Double, Integer))]
    combined_by_gene = union_rdd.combineByKey(
        #createCombiner as C
        lambda v: (v, 1),
             
        # multiplyAndCount
        lambda C, v: (C[0]*v, C[1]+1),
             
        # mergeCombiners
        lambda C, D: (C[0]*D[0], C[1]+D[1])
    )
                     
    # next calculate ranked products and the number of elements
    ranked_products = combined_by_gene.mapValues(
        lambda v : (pow(float(v[0]), float(v[1])), v[1])
    )
            
    return ranked_products
#end-def
#-------------------------------------------
# @param rdd : RDD[(String, Double)]
# returns: RDD[(String, Long)] : (gene_id, rank)
def assign_rank(rdd):
    # swap key and value (will be used for sorting by key)
    # convert value to abs(value)   
    swapped_rdd = rdd.map(lambda v: (abs(v[1]), v[0]))
        
    # sort copa scores descending
    # we need 1 partition so that we can zip numbers into this RDD by zipWithIndex()
    # If we do not use 1 partition, then indexes will be meaningless
    # sorted_rdd : RDD[(Double,String)]
    sorted_rdd = swapped_rdd.sortByKey(False, 1)
    print("sorted_rdd", sorted_rdd.collect()) 
    
    # use zipWithIndex()
    # Long values will be 0, 1, 2, ...
    # for ranking, we need 1, 2, 3, ..., therefore, 
    # we will add 1 when calculating the ranked product
    # indexed :  RDD[((Double,String), Long)] 
    indexed = sorted_rdd.zipWithIndex()
    print("indexed", indexed.collect()) 

    # add 1 to index
    # ranked :  RDD[(String, Long)]       
    ranked = indexed.map(lambda v: (v[0][1], v[1]+1))
    print("ranked", ranked.collect()) 
    return ranked
#end-def
#-------------------------------------------
# Input parameters:
#        sys.argv[1]   = output path
#        sys.argv[2]   = number of studies (K)
#        sys.argv[3]   = input path for study 1
#        sys.argv[4]   = input path for study 2
#        ...
#        sys.argv[K+2] = input path for study K

#  CREATE an instance of a SparkSession object
spark = SparkSession\
    .builder\
    .appName("rank product")\
    .getOrCreate()
    
    
# Handle input parameters
output_path = sys.argv[1] 
print("output_path", output_path)

# set K = number of studies
K = int(sys.argv[2]) 
print("K", K)
#    
# define studies_input_path
studies_input_path = [sys.argv[i+3] for i in range(K)]
print("studies_input_path", studies_input_path)


# Step-1: Perform Rank Product
# Spark requires an array for creating union of many RDDs
# means[i] : RDD[(String, Double)]
means = [compute_mean(studies_input_path[i]) for i in range(K)] 

# Step-2: Compute rank
#   1. sort values based on absolute value of copa scores: 
#      to sort by copa score, we will swap K with V and then sort by key
#   2. assign rank from 1 (to highest copa score) to n (to the lowest copa score)
#   3. calcluate rank for each gene_id as Math.power(R1 * R2 * ... * Rn, 1/n)
ranks = [assign_rank(means[i]) for i in range(K)]

# Step-3: Calculate ranked products
# ranked_products  : RDD[(gene_id, (ranked_product, N))]
ranked_products = compute_ranked_products(ranks) 
             
# Step-4: save the result
ranked_products.saveAsTextFile(output_path) 

# done
spark.stop()
