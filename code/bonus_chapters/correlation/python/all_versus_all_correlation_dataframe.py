from __future__ import print_function 
from pyspark.sql import SparkSession 
import sys 
from scipy import stats
from collections import Counter

from pyspark.sql.types import MapType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import array_contains
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql import Row


#-----------------------------------------------------
# All-vs-All Correlation: DataFrame Solution
#------------------------------------------------------
# This code implements Pearson correlation and 
# Spearman correlation for a set of genes.
#
# NOTE: print(), RDD.collect(), and DataFrame.show() 
# are used for debugging and educational purposes.
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
# list_of_tuples2 = [(String, Float)] 
#                 = [(patient_id, biomarker_value)]
#                 = [Row(patient_id='p1', biomarker_value=0.2), Row(patient_id='p1', biomarker_value=2.3), ...]
# return dictionary[(k, v)]
# where 
#   k: patient_id
#   v: average of biomarker values for a single patient_id
#
def to_dictionary(list_of_tuples2):
    # print("to_dictionary(): list_of_tuples2=", str(list_of_tuples2))
    hashmap = {}
    for t2 in list_of_tuples2:
        # print("to_dictionary(): t2=", str(t2))

        patient_id = t2.patient_id
        biomarker_value = t2.biomarker_value
        # print("to_dictionary(): patient_id=", str(patient_id))
        # print("to_dictionary(): biomarker_value=", str(biomarker_value))

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
    
    # print("to_dictionary(): avg_map=", str(avg_map))
    return avg_map;
#end-def
#------------------------------------------------------------
# x= [x1, x2, ...] = [Row(patient_id='p1', biomarker_value=0.2), Row(patient_id='p1', biomarker_value=2.3), ...]
# y= [y1, y2, ...] = [Row(patient_id='p1', biomarker_value=1.2), Row(patient_id='p1', biomarker_value=1.3), ...]
def stats_pearsonr(x, y):
    print("x=", str(x))
    print("y=", str(y))
    
    # correlation does not make sense if you have less than 2 values
    if (len(x) < 2): return (None, None)
    return stats.pearsonr(x, y)
#end-def
#------------------------------------------------------------
# x = [x1, x2, ...]
# y = [y1, y2, ...]
def stats_spearmanr(x, y):
    # correlation does not make sense if you have less than 2 values
    if (len(x) < 2): return (None, None)
    return stats.spearmanr(x, y)
#end-def
#---------------------------------------------------------------
#
#  values_1 = Iterable<(patient_id, biomarker_value)>
#  values_2 = Iterable<(patient_id, biomarker_value)>
#
# return (correlation, pvalue)
#
def calculate_pearson_correlation(values_1, values_2):
    # print("calculate_pearson_correlation() values_1 = ", str(values_1))
    # print("calculate_pearson_correlation() values_2 = ", str(values_2))

    #
    # for v1 in values_1: print("calculate_pearson_correlation() v1=", str(v1))
    # for v2 in values_2: print("calculate_pearson_correlation() v2=", str(v2))

    g1_dict = to_dictionary(values_1);
    g2_dict = to_dictionary(values_2);
    #
    #now perform a correlation(one, other)
    # make sure we order the values accordingly by patient_id
    x = [] # List<Double>()
    y = [] # List<Double>()
    for g1_patient_id, g1_avg in g1_dict.items():
        g2_avg = g2_dict[g1_patient_id]
        if (g2_avg is None): continue
        # both gene1 and gene2 for patient_id have values
        x.append(g1_avg)
        y.append(g2_avg)
    #end-for

    # calculate Pearson correlation and pvalue
    pearson_correlation, pearson_pvalue = stats_pearsonr(x, y)
    print("calculate_pearson_correlation() pearson_correlation = ", str(pearson_correlation))
    print("calculate_pearson_correlation() pearson_pvalue = ", str(pearson_pvalue))

    return {"correlation" : float(pearson_correlation), "pvalue" : float(pearson_pvalue)}
    # return Row(correlation=pearson_correlation, pvalue=pearson_pvalue)
    # return  (pearson_correlation, pearson_pvalue)
    # return [("correlation", pearson_correlation), ("pvalue", pearson_pvalue)]
    # return str(pearson_correlation) + "," + str(pearson_pvalue)

#end-def
#---------------------------------------------------------------
#
#  values_1 = Iterable<(patient_id, biomarker_value)>
#  values_2 = Iterable<(patient_id, biomarker_value)>
#
# return (correlation, pvalue)
#
def calculate_spearman_correlation(values_1, values_2):
    # print("calculate_spearman_correlation() values_1 = ", str(values_1))
    # print("calculate_spearman_correlation() values_2 = ", str(values_2))

    #
    # for v1 in values_1: print("calculate_spearman_correlation() v1=", str(v1))
    # for v2 in values_2: print("calculate_spearman_correlation() v2=", str(v2))

    g1_dict = to_dictionary(values_1);
    g2_dict = to_dictionary(values_2);
    #
    #now perform a correlation(one, other)
    # make sure we order the values accordingly by patient_id
    x = [] # List<Double>()
    y = [] # List<Double>()
    for g1_patient_id, g1_avg in g1_dict.items():
        g2_avg = g2_dict[g1_patient_id]
        if (g2_avg is None): continue
        # both gene1 and gene2 for patient_id have values
        x.append(g1_avg)
        y.append(g2_avg)
    #end-for

    # calculate Spearman correlation and pvalue
    spearman_correlation, spearman_pvalue = stats_spearmanr(x, y)
    print("calculate_spearman_correlation() spearman_correlation = ", str(spearman_correlation))
    print("calculate_spearman_correlation() spearman_pvalue = ", str(spearman_pvalue))

    return {"correlation" : float(spearman_correlation), "pvalue" : float(spearman_pvalue)}
    # return str(spearman_correlation) + "," + str(spearman_pvalue)
#end-def
#---------------------------------------------------------------

def main():

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # define input path
    input_path = sys.argv[1]
    print("input_path = ", input_path)
    
    # define data  schema
    data_schema = StructType() \
      .add("gene_id", StringType(), True) \
      .add("patient_id", StringType(),True) \
      .add("biomarker_value", DoubleType(),True) 
      
    # create df : DataFrame(gene_id, patient_id, biomarker_value)
    df = spark.read.format("csv").schema(data_schema).load(input_path)
    print("df.count() = ", df.count())
    df.show(20, truncate=False)
    df.printSchema()

    
    # group by gene_id
    # grouped_by_gene_id: RDD[(gene_id, Iterable<(patient_id, biomarker_value)>)]
    grouped_by_gene_id = df.groupBy("gene_id").agg(F.collect_list(F.struct(["patient_id","biomarker_value"])).alias("values"))
    print("grouped_by_gene_id.count() = ", grouped_by_gene_id.count())
    grouped_by_gene_id.show(20, truncate=False)
    grouped_by_gene_id.printSchema()

    
    # Perform G.cartesian(G)
    # to find the correlation of (a, b) where a in G and b in G
    # we find the cartesian of grouped_by_gene_id by itself
    # then we filter out duplicate pairs
    # cart : RDD[(X, Y)] where
    #        X = (gene_id, Iterable<(patient_id, biomarker_value)>), 
    #        Y = (gene_id, Iterable<(patient_id, biomarker_value)>)
    cart = grouped_by_gene_id.crossJoin(grouped_by_gene_id.withColumnRenamed('gene_id', 'gene_id_2').withColumnRenamed('values', 'values_2'))
    print("cart = ", cart)
    print("cart.count() = ", cart.count())
    cart.show(20, truncate=False)
    cart.printSchema()
    """
    filtered schema:
     |-- gene_id: string (nullable = true)
     |-- values: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- patient_id: string (nullable = true)
     |    |    |-- biomarker_value: double (nullable = true)
     |-- gene_id_2: string (nullable = true)
     |-- values_2: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- patient_id: string (nullable = true)
     |    |    |-- biomarker_value: double (nullable = true)      
    """
 
  
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
    filtered = cart.filter(col("gene_id") < col("gene_id_2"))
    print("filtered = ", filtered)
    print("filtered.count() = ", filtered.count())
    filtered.show(20, truncate=False)
    filtered.printSchema()
    """
    filtered schema:
     |-- gene_id: string (nullable = true)
     |-- values: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- patient_id: string (nullable = true)
     |    |    |-- biomarker_value: double (nullable = true)
     |-- gene_id_2: string (nullable = true)
     |-- values_2: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- patient_id: string (nullable = true)
     |    |    |-- biomarker_value: double (nullable = true)      
    """ 
 
    # define a return type for UDFs
    #result_schema = ArrayType(StructType([StructField("key",StringType(),True), \
    #                                      StructField("value",DoubleType(),True)]))
    result_schema = MapType(StringType(), FloatType(), True)                              
    # create a UDF from a python function:
    # StructType() is a return type 
    calc_pearson_corr_udf = udf(lambda v1, v2: calculate_pearson_correlation(v1, v2), result_schema)
    # calc_pearson_corr_udf = udf(lambda v1, v2: calculate_pearson_correlation(v1, v2), StringType())
        
    # create a UDF from a python function:
    # StructType() is a return type 
    calc_spearman_corr_udf = udf(lambda v1, v2: calculate_spearman_correlation(v1, v2), result_schema)
    # calc_spearman_corr_udf = udf(lambda v1, v2: calculate_spearman_correlation(v1, v2), StringType())
     
    corr = filtered.withColumn("pearson", calc_pearson_corr_udf(filtered.values, filtered.values_2))\
                   .withColumn("spearman", calc_spearman_corr_udf(filtered.values, filtered.values_2))\
                   .drop("values")\
                   .drop("values_2")
    
    print("corr = ", corr)
    print("corr.count() = ", corr.count())
    corr.show(20, truncate=False)
    corr.printSchema()
    
    # done
    spark.stop();
#end-def
#---------------------------------------------------------------

if __name__ == '__main__':
    main()

   
