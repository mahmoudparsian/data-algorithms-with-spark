# Chapter 4

## Programs

The goal of the programs in this chapter is
to show some of the important reductions 
in Spark. Some of the reductions by key
are:

* `reduceBykey()`
* `combineBykey()`
* `groupBykey()`
* `aggregateBykey()`

## Average per Key

We want to find out average per key in PySpark. 
The solutions are presented in this chapter as:

* `combineByKey()`:
	* `average_by_key_use_combinebykey.py` (PySpark program) 
	* `average_by_key_use_combinebykey.sh` (shell script to call PySpark)

* `groupByKey()`:
	* `average_by_key_use_groupbykey.py` (PySpark program)
	* `average_by_key_use_groupbykey.sh` (shell script to call PySpark)

* `reduceByKey()`:
	* `average_by_key_use_reducebykey.py` (PySpark program)
	* `average_by_key_use_reducebykey.sh` (shell script to call PySpark)

* `aggregateByKey()`:
	* `average_by_key_use_aggregatebykey.py` (PySpark program)
	* `average_by_key_use_aggregatebykey.sh` (shell script to call PySpark)

## Median per Key

We want to find out average per key in PySpark. 
The solutions are presented in this chapter as:

* `combineByKey()`:
	* `exact_median_by_key_use_combinebykey.py` (PySpark program) 

* `groupByKey()`:
	* `exact_median_by_key_use_groupbykey.py` (PySpark program)

* `reduceByKey()`:
	* `exact_median_by_key_use_reducebykey.py` (PySpark program)

* `aggregateByKey()`:
	* `exact_median_by_key_use_aggregatebykey.py` (PySpark program)
