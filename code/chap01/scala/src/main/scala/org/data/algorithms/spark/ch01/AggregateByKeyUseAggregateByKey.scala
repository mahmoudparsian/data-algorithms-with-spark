package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

object AggregateByKeyUseAggregateByKey {

  /*Apply a aggregateByKey() transformation to an
  RDD[(key, value)] to find average per key.
  Input: NONE
    Use collections to crete RDD[(key, value)]
  ------------------------------------------------------
  Input Parameters:
     NONE
  -------------------------------------------------------
  @author Deepak Kumar
  -------------------------------------------------------

  =========================================
  Create a pair of (name, number) from t3
  t3 = (name, city, number)*/
  def createPair(t3: (String, String, Int)): (String, Int) = {
    val name = t3._1
    val number = t3._3.intValue()
    (name, number)

  }

  /* end-def
  ==========================================*/
  def main(args: Array[String]) = {
    //create an instance of SparkSession
    val spark =
      SparkSession
        .builder()
        .appName("Aggregate By Key")
        .master("local[*]")
        .getOrCreate()

    /* ========================================
    aggregateByKey() transformation

    source_rdd.aggregateByKey(zeroValue, seqOp, combOp) --> target_rdd

    aggregateByKey(
      zeroValue,
      seqFunc,
      combFunc,
      numPartitions=None,
      partitionFunc=<function portable_hash>
    )

    Aggregate the values of each key, using given combine
    functions and a neutral "zero value". This function can
    return a different result type, U, than the type of the
    values in this RDD, V. Thus, we need one operation for
    merging a V into a U and one operation for merging two U's,
    The former operation is used for merging values within a
    partition, and the latter is used for merging values between
    partitions. To avoid memory allocation, both of these functions
    are allowed to modify and return their first argument instead
    of creating a new U.

    We will use (SUM, COUNT) as a combined data structure
    for finding the sum of the values and count of the
    values per key. By KEY, simultaneously calculate the SUM
    (the numerator for the average that we want to compute),
    and COUNT (the denominator for the average that we want
    to compute):

    zero_value = (0,0)

    rdd2 = rdd1.aggregateByKey(
       zero_value,
        (a,b) => (a._1 + b,    a._2 + 1),
        (a,b) => (a._1 + b._1, a._2 + b._2)
    )

    Where the following is true about the meaning of each a and b
    pair above (so you can visualize what's happening):

    First lambda expression for Within-Partition Reduction Step::
       a: is a TUPLE that holds: (runningSum, runningCount).
       b: is a SCALAR that holds the next Value

    Second lambda expression for Cross-Partition Reduction Step::
       a: is a TUPLE that holds: (runningSum, runningCount).
       b: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).

    Finally, calculate the average for each KEY, and collect results.

     finalResult = rdd2.mapValues( v => v._1/v._2).collect()
    =======================================

    Create a list of tuples.
    Each tuple contains name, city, and age.
    Create a RDD from the list above.*/
    val listOfTuples = List(("alex", "Sunnyvale", 25),
      ("alex", "Sunnyvale", 33),
      ("alex", "Sunnyvale", 45),
      ("alex", "Sunnyvale", 63),
      ("mary", "Ames", 22),
      ("mary", "Cupertino", 66),
      ("mary", "Ames", 20),
      ("bob", "Ames", 26)
    )
    val rdd = spark.sparkContext.parallelize(listOfTuples)
    println("rdd = " + rdd)
    println("rdd.collect()=" + rdd.collect())
    println("rdd.count()=" + rdd.count())
    rdd.collect.foreach(println)

    /* ------------------------------------
    apply a map() transformation to rdd
    create a (key, value) pair
     where
          key is the name (first element of tuple)
          value is a number
     ------------------------------------*/
    val rdd2 = rdd.map(createPair)
    println("rdd2 = ", rdd2)
    println("rdd2.count() = ", rdd2.count())
    println("rdd2.collect() = ", rdd2.collect())


    /*------------------------------------
    apply a aggregateByKey() transformation to rdd2
    create a (key, value) pair
     where
          key is the name
          value is the (sum, count)
    ------------------------------------
    v : a number for a given (key, v) pair of source RDD
    C : is a tuple of pair (sum, count), which is
    also called a "combined" data structure

    (0, 0) denotes a zero value per partition
    where (0, 0 ) = (sum, count)
    Each partition starts with (0, 0) as an initial value
    */

    val sumCount = rdd2.aggregateByKey(
      (0, 0))(
      (C, v) => (C._1 + v, C._2 + 1),
      (C1, C2) => (C1._1 + C2._1, C1._2 + C2._2)
    )
    println("sum_count = " + sumCount)
    println("sum_count.count() =" + sumCount.count())
    println("sum_count.collect() = ", sumCount.collect())

    /*===============================
     find average per key
     sum_count = [(k1, (sum1, count1)), (k2, (sum2, count2)), ...]
    ===============================
    */


    val averages = sumCount.mapValues(sumAndCount => (sumAndCount._1).floatValue() / (sumAndCount._2).floatValue())
    println("averages = ", averages)
    averages.collect().foreach(println(_))
    println("averages.count() = ", averages.count())
    println("averages.collect() = ", averages.collect())
    //done!
    spark.stop()
  }
}



