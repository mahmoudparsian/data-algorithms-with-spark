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

We want to find out average per key in Spark    .
The solutions are presented in this chapter as:

* `combineByKey()`:
    * `org.data.algorithms.spark.ch04.AverageByKeyUseCombineByKey` (Spark program)
    * `./run_spark_applications_scripts/average_by_key_use_combine_by_key.sh` (shell script to call Spark Application)

* `groupByKey()`:
    * `org.data.algorithms.spark.ch04.AverageByKeyUseGroupByKey` (Spark program)
    * `./run_spark_applications_scripts/average_by_key_use_group_by_key.sh` (shell script to call Spark Application)

* `reduceByKey()`:
    * `org.data.algorithms.spark.ch04.AverageByKeyUseReduceByKey` (Spark program)
    * `./run_spark_applications_scripts/average_by_key_use_reduce_by_key.sh` (shell script to call Spark Application)

* `aggregateByKey()`:
    * `org.data.algorithms.spark.ch04.AverageByKeyUseAggregateByKey` (Spark program)
    * `./run_spark_applications_scripts/average_by_key_use_aggregate_by_key.sh` (shell script to call Spark Application)

## Median per Key

We want to find out median per key in Spark.
The solutions are presented in this chapter as:

* `combineByKey()`:
    * `org.data.algorithms.spark.ch04.ExactMedianByKeyUseCombineByKey` (Spark program)
    * `./run_spark_applications_scripts/exact_median_by_key_use_combine_by_key.sh` (shell script to call Spark Application)


* `groupByKey()`:
    * `org.data.algorithms.spark.ch04.ExactMedianByKeyUseGroupByKey` (Spark program)
    * `./run_spark_applications_scripts/exact_median_by_key_use_group_by_key.sh` (shell script to call Spark Application)

* `reduceByKey()`:
    * `org.data.algorithms.spark.ch04.ExactMedianByKeyUseReduceByKey` (Spark program)
    * `./run_spark_applications_scripts/exact_median_by_key_use_reduce_by_key.sh` (shell script to call Spark Application)

* `aggregateByKey()`:
    * `org.data.algorithms.spark.ch04.ExactMedianByKeyUseAggregateByKey` (Spark program)
    * `./run_spark_applications_scripts/exact_median_by_key_use_aggregate_by_key.sh` (shell script to call Spark Application)
