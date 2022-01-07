from __future__ import print_function 
from pyspark.sql import SparkSession 
import sys 
from scipy import stats

#-----------------------------------------------------
# All-vs-All Correlation: RDD Solution
#------------------------------------------------------
# This code implements Pearson correlation and 
# Spearman correlation for a set of genes.
#
# NOTE: print() and RDD.collect() are used 
# for debugging and educational purposes.
#------------------------------------------------------
#
# Pearson correlation:
#    scipy.stats.pearsonr(x, y)
#    Calculates a Pearson correlation coefficient and the p-value for testing non-correlation.
#    source : https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html
#
# Spearman correlation:
#    scipy.stats.spearmanr(a, b=None, axis=0, nan_policy='propagate', alternative='two-sided')
#    Calculate a Spearman correlation coefficient with associated p-value.
#    source: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.spearmanr.html
#------------------------------------------------------
#
# Input Parameters: text file (s)
# 
#-------------------------------------------------------
#
# @author Mahmoud Parsian
#
#-------------------------------------------------------
"""
What does All-vs-All correlation means?

Let selected genes/items/movies be: G = (g1, g2, g3, g4, ...).
 
Then all-vs-all will correlate between the following:

    (g1, g2)
    (g1, g3)
    (g1, g4)
    (g2, g3)
    (g2, g4)
    (g3, g4)
  ...

How to implement efficient correlation algorithms? 
Avoid duplicate pair calculations: Let a and b in G.
Then pairs (a, b) are generated if and only if (a < b).
We assume that correlation(a, b) = correlation(b, a)
The goal is not to generate duplicate pairs (a, b) and (b, a),
but just to generate (a, b) if (a < b).

There are many correlation algorithms: 
    Pearson
    Spearman
    ...
    
Input Data Format:
    <gene_id_as_string><,><patient_id_as_string><,><biomarker_value_as_float>

Record Example:
    g37761,2,patient_1234,1.2872

 """
 #------------------------------------------------------------
# Return true if g1 comes before g2 (to make sure not 
# to create duplicate pair of genes).
# e: ((gene_id_1, [...]), [gene_id, [...]])

def is_smaller(e):
    gene1_id = e[0][0]
    gene2_id = e[1][0]
    return is_smaller_gene(gene1_id, gene2_id)
#end-def
#------------------------------------------------------------
# Return true if g1 comes before g2 (to make sure not 
# to create duplicate pair of genes).
def is_smaller_gene(g1, g2):
    if g1 < g2:
        return True
    else:
        return False
#end-def
#------------------------------------------------------------
# rec: <gene_id_as_string><,><patient_id_as_string><,><biomarker_value_as_float>
# create pair as: (gene_id, (patient_id, biomarker_value))
def create_pair(rec):
    tokens = rec.split(",")
    #
    gene_id = tokens[0]
    patient_id = tokens[1]
    biomarker_value = float(tokens[2])
    #
    return (gene_id, (patient_id, biomarker_value))
#end-def
#------------------------------------------------------------
# list_of_tuples2 = [(String, Float)] = [(patient_id, biomarker_value)]
# return dictionary[(k, v)]
# where 
#   k: patient_id
#   v: average of biomarker values for a single patient_id
#
def to_dictionary(list_of_tuples2):
    hashmap = {}
    for t2 in list_of_tuples2:
        patient_id = t2[0]
        biomarker_value = t2[1]
        #
        if patient_id in hashmap:
            # hashmap[patient_id] exists
            hashmap[patient_id].append(biomarker_value)
        else:
            hashmap[patient_id]= [biomarker_value]
        #end-if
    #end-for
    
    # now calculate average per patient_id
    avg_map = {}
    for patient_id, list_of_values in hashmap.items():
        avg_map[patient_id] = float(sum(list_of_values)) / len(list_of_values)
    #end-for
    
    return avg_map;
#end-def
#------------------------------------------------------------
# x = [x1, x2, ...]
# y = [y1, y2, ...]
def calc_pearson_correlation(x, y):
    # correlation does not make sense if you have less than 2 values
    if (len(x) < 2): return (None, None)
    return stats.pearsonr(x, y)
#end-def
#------------------------------------------------------------
# x = [x1, x2, ...]
# y = [y1, y2, ...]
def calc_spearman_correlation(x, y):
    # correlation does not make sense if you have less than 2 values
    if (len(x) < 2): return (None, None)
    return stats.spearmanr(x, y)
#end-def
#------------------------------------------------------------
#  e: (gene1, gene2)
#  where 
#        gene1 = (gene_id_1, Iterable<(patient_id, biomarker_value)>), 
#        gene2 = (gene_id_2, Iterable<(patient_id, biomarker_value)>)  
#
#  create (K,V), 
#  where 
#       K = (gene_id_1, gene_id_2)
#       V = (correlation, pvalue)
#
def calculate_correlation(e):
    gene1 = e[0]
    gene2 = e[1]
    
    # build a key as (gene_id_1, gene_id_2)
    key = (gene1[0], gene2[0]) 
    #
    g1_dict = to_dictionary(gene1[1]);
    g2_dict = to_dictionary(gene2[1]);
    #
    #now perform a correlation(one, other)
    # make sure we order the values accordingly by patient_id
    x = [] # List<Double>();
    y = [] # List<Double>();
    for g1_patient_id, g1_avg in g1_dict.items():
        g2_avg = g2_dict[g1_patient_id]
        if (g2_avg is None): continue
        # both gene1 and gene2 for patient_id have values
        x.append(g1_avg)
        y.append(g2_avg)
    #end-for

    # calculate Pearson correlation and pvalue
    pearson_correlation, pearson_pvalue = calc_pearson_correlation(x, y)
                    
    # Calculate Spearman correlation and pvalue
    spearman_correlation, Spearman_pvalue = calc_spearman_correlation(x, y)
    #
    return (key, pearson_correlation, pearson_pvalue, spearman_correlation, Spearman_pvalue)
#end-def
#---------------------------------------------------------------
def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # define input path
    input_path = sys.argv[1]
    print("input_path = ", input_path)

    # create rdd : RDD[String]
    rdd = spark.sparkContext.textFile(input_path)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())

    # create RDD[(gene_id, (patient_id, biomarker_value))]
    pairs = rdd.map(create_pair)
    print("pairs = ", pairs)
    print("pairs.count() = ", pairs.count())
    print("pairs.collect() = ", pairs.collect())
    
    # group by gene_id
    # grouped_by_gene_id: RDD[(gene_id, Iterable<(patient_id, biomarker_value)>)]
    grouped_by_gene_id = pairs.groupByKey();
    print("grouped_by_gene_id = ", grouped_by_gene_id)
    print("grouped_by_gene_id.count() = ", grouped_by_gene_id.count())
    print("grouped_by_gene_id.collect() = ", grouped_by_gene_id.mapValues(lambda v: list(v)).collect())
    
    
    # Perform G.cartesian(G)
    # to find the correlation of (a, b) where a in G and b in G
    # we find the cartesian of grouped_by_gene_id by itself
    # then we filter out duplicate pairs
    # cart : RDD[(X, Y)] where
    #        X = (gene_id, Iterable<(patient_id, biomarker_value)>), 
    #        Y = (gene_id, Iterable<(patient_id, biomarker_value)>)
    cart = grouped_by_gene_id.cartesian(grouped_by_gene_id)
    print("cart = ", cart)
    print("cart.count() = ", cart.count())
    print("cart.collect() = ", cart.map(lambda x: ( (x[0][0], list(x[0][1])), (x[1][0], list(x[1][1])) )).collect())
    # given G = {g1, g2, g3, g4}
    # cart = 
    #        (g1, [g1]), (g1, [g2]), (g1, [g3]), (g1, [g4]) 
    #        (g2, [g1]), (g2, [g2]), (g2, [g3]), (g2, [g4]) 
    #        (g3, [g1]), (g3, [g2]), (g3, [g3]), (g3, [g4]) 
    #        (g4, [g1]), (g4, [g2]), (g4, [g3]), (g4, [g4])    
    
    # filter it and keep the ones (a, b)  if and only if (a < b).
    # after filtering, we will have:
    # filtered =
    #        (g1, g2),  (g1, g3), (g1, g4)     
    #        (g2, g3), (g2, g4)     
    #        (g3, g4)   
    # filter : RDD[(X, Y)] where
    #        X.gene_id < Y.gene_id
    #        X = (gene_id, Iterable<(patient_id, biomarker_value)>), 
    #        Y = (gene_id, Iterable<(patient_id, biomarker_value)>)  
    filtered = cart.filter(is_smaller)
    print("filtered = ", filtered)
    print("filtered.count() = ", filtered.count())
    print("filtered.collect() = ", filtered.map(lambda x: ( (x[0][0], list(x[0][1])), (x[1][0], list(x[1][1])) )).collect())
    
    # corr = (K, V) where
    # K = (g1, g2)
    # V = (correlation, pvalue)
    corr = filtered.map(calculate_correlation)
    print("corr = ", corr)
    print("corr.count() = ", corr.count())
    print("corr.collect() = ", corr.collect())
    
    # done
    spark.stop();
#end-def
#---------------------------------------------------------------

if __name__ == '__main__':
    main()

   
"""
How does Cartesian work?

>>> mylist = [('g1', [1, 11]), ('g2', [2, 22]), ('g3', [3, 33])]
>>> rdd = spark.sparkContext.parallelize(mylist)
>>> rdd.collect()
[('g1', [1, 11]), ('g2', [2, 22]), ('g3', [3, 33])]
>>> cart = rdd.cartesian(rdd)
>>> cart.mapValues(lambda v: list(v)).collect()
[
 (('g1', [1, 11]), ['g1', [1, 11]]), 
 (('g1', [1, 11]), ['g2', [2, 22]]), 
 (('g1', [1, 11]), ['g3', [3, 33]]), 
 (('g2', [2, 22]), ['g1', [1, 11]]), 
 (('g2', [2, 22]), ['g2', [2, 22]]), 
 (('g2', [2, 22]), ['g3', [3, 33]]), 
 (('g3', [3, 33]), ['g1', [1, 11]]), 
 (('g3', [3, 33]), ['g2', [2, 22]]), 
 (('g3', [3, 33]), ['g3', [3, 33]])
]
"""