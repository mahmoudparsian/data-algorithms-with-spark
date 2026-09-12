# Correlation

## Introduction

Correlation is a statistical measure that indicates the extent to which 
two or more variables fluctuate in relation to each other. A positive 
correlation indicates the extent to which those variables increase or 
decrease in parallel; a negative correlation indicates the extent to which 
one variable increases as the other decreases.

Possible values of the correlation coefficient range from `-1` to `+1`, 
with `-1` indicating a perfectly linear negative, i.e., inverse, correlation 
(sloping downward) and `+1` indicating a perfectly linear positive correlation 
(sloping upward). A correlation coefficient close to 0 suggests little, if any, 
correlation.

-----

## Problem Statement

What does All-vs-All correlation means?

Let selected genes/items/movies be: `G = {g1, g2, g3, g4, ...}`
 
Then all-vs-all will correlate between the following:

    
    (g1, g2)
    (g1, g3)
    (g1, g4)
    (g2, g3)
    (g2, g4)
    (g3, g4)
    ...
    

The goal is to find the following output:
    
    
    ((g1, g2), (pearson_correlation, spearman_correlation))
    ((g1, g3), (pearson_correlation, spearman_correlation))
    ((g1, g4), (pearson_correlation, spearman_correlation))
    ((g2, g3), (pearson_correlation, spearman_correlation))
    ((g2, g4), (pearson_correlation, spearman_correlation))
    ((g3, g4), (pearson_correlation, spearman_correlation))
    ...


How to implement efficient correlation algorithms? 
Avoid duplicate pair calculations: Let `a` and `b` in `G`.
Then pairs `(a, b)` are generated if and only if `(a < b)`.
We assume that `correlation(a, b) = correlation(b, a)`.
The goal is not to generate duplicate pairs `(a, b)` and `(b, a)`,
but just to generate `(a, b)` if `(a < b)`.

There are many correlation algorithms: 

* Pearson
* Spearman
    ...
    
### Pearson correlation:

`scipy.stats.pearsonr(x, y)`

Calculates a [Pearson correlation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html) 
coefficient and the p-value for testing non-correlation.
     

### Spearman correlation:

`scipy.stats.spearmanr(x, y)`

Calculate a [Spearman correlation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.spearmanr.html) 
coefficient with associated p-value.


### Input Data Format:

    <gene_id_as_string><,><patient_id_as_string><,><biomarker_value_as_float>

    Record Example:
         g37761,patient_1234,1.28
         g22761,patient_1234,0.28
         g37761,patient_4788,1.20

-----

![K-mer](https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/images/correlation-coefficient.png)