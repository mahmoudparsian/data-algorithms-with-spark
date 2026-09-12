package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * For testing and debugging:
 *   To force empty partitions, set the number of
 *   partitions higher than the number of records.
 *   The purpose is to test handling empty partitions.
 *-----------------------------------------------------
 * Find Minimum and Maximum of all input by
 * using the mapPartitions() transformations.
 *
 * The idea is that each partition will find
 * (local_min, local_max, local_count)
 * and then we find (final_min, final_max, final_count)
 * for all partitions.
 *
 * input ---- partitioned ---->  partition-1, partition-2, ...
 *
 * partition-1 => local1 = (local_min1, local_max1, local_count1)
 * partition-2 => local2 = (local_min2, local_max2, local_count2)
 * ...
 *
 * final_min_max = minmax(local1, local2, ...)
 *
 *------------------------------------------------------
 * Input Parameters:
 *    INPUT_PATH as a file of numbers
 *
 * Example: sample_numbers.txt
 *
 * $ cat sample_numbers.txt
 *23,24,22,44,66,77,44,44,555,666
 *12,4,555,66,67,68,57,55,56,45,45,45,66,77
 *34,35,36,97300,78,79
 *120,44,444,445,345,345,555
 *11,33,34,35,36,37,47,7777,8888,6666,44,55
 *10,11,44,66,77,78,79,80,90,98,99,100,102,103,104,105
 *6,7,8,9,10
 *8,9,10,12,12
 *7777
 *222,333,444,555,666,111,112,5,113,114
 *5555,4444,24
 *
 *
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object MinMaxForceEmptyPartitions {

  /**
   *------------------------------------------
   * Find (min, max, count) for a given single partition.
   *
   * partitionIterator is an iterator over
   * elements of a single partition.
   * partitionIterator : iterator over
   * set of input records and each input record
   * has the format as:
   * <number><,><number><,>...<number>
   *
   */
  def minmax(partitionIterator: Iterator[String]): Iterator[(Int, Int, Int)] = {
    var firstRecord = ""
    try {
      firstRecord = partitionIterator.next()
      println("firstRecord=" + firstRecord)
    } catch {
      case _: NoSuchElementException =>
        return Iterator((1, -1, 0)) // WHERE min > max to filter out later
    }
    var numbers = firstRecord.split(",").map(_.toInt)
    var localMin = numbers.min
    var localMax = numbers.max
    var localCount = numbers.length

    // handle remaining records in a partition
    for (record <- partitionIterator) {
      numbers = record.split(",").map(_.toInt)
      val min2 = numbers.min
      val max2 = numbers.max
      // update min, max, and count
      localCount += numbers.length
      localMax = Math.max(localMax, max2)
      localMin = Math.min(localMin, min2)
    }
    Iterator((localMin, localMax, localCount))
  }

  /**
   *------------------------------------------
   *
   * find final (min, max, count) from all partitions
   * and filter out (1, -1, 0) tuples. Note that we
   * created (1, -1, 0) from empty partitions
   * min_max_count_list = [
   *                       (min1, max1, count1),
   *                       (min2, max2, count2),
   *                       ...
   *                      ]
   *
   */
  def findMinMaxCount(minMaxCountList: Array[(Int, Int, Int)]): (Int, Int, Int) = {
    var firstTime = true
    var finalMin = 0
    var finalMax = 0
    var finalCount = 0
    //  iterate tuple3 in minMaxCountList:
    for ((localMin, localMax, localCount) <- minMaxCountList) {
      // filter out (1, -1, 0) tuples
      // to handle empty partitions
      if (localMin <= localMax)
        if (firstTime) {
          finalMin = localMin
          finalMax = localMax
          finalCount = localCount
          firstTime = false
        } else {
          finalMin = Math.min(finalMin, localMin)
          finalMax = Math.max(finalMax, localMax)
          finalCount += localCount
        }
    }
    (finalMin, finalMax, finalCount)
  }

  def debugPrint(iterator: Iterator[String]) : Unit = {
    println("===begin-partition===")
    for (x <- iterator) println(x)
    println("===end-partition===")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("InputPath is missing")
      System.exit(-1)
    }

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    println("spark=" +  spark)

    // handle input parameter
    val inputPath = args(0)
    println("inputPath=" + inputPath)

    /**
     *-------------------------------------
     * read input and apply mapPartitions()
     *-------------------------------------
     *
     * force empty partitions by
     * setting a high number of partitions
     * for this input sample_numbers.txt
     *
     */
    val NUM_OF_PARTITIONS = 16
    val rdd = spark.sparkContext.textFile(inputPath, NUM_OF_PARTITIONS)
    println("rdd=" +  rdd)
    println("rdd.count=" +  rdd.count())
    println("rdd.collect()=" +  rdd.collect().mkString("Array(", ", ", ")"))
    println("rdd.getNumPartitions()=" +  rdd.getNumPartitions)

    /**
     *
     *-------------------------------------
     * find (min, max, count) per partition
     *-------------------------------------
     */
    val minMaxCount = rdd.mapPartitions(minmax)
    println("minMaxCount=" +  minMaxCount)
    println("minMaxCount.count=" +  minMaxCount.count())
    val minMaxCountList = minMaxCount.collect()
    println("minMaxCount.collect()=" +  minMaxCountList.mkString("Array(", ", ", ")"))

    /**
     *-------------------------------------
     * find final (min, max, count) from all partitions
     * and filter out (1, -1, 0) tuples
     *-------------------------------------
     */

    val (finalMin, finalMax, finalCount) = findMinMaxCount(minMaxCountList)
    println("final: (min, max, count)= (" + finalMin + ", " + finalMax + ", " + finalCount + ")")

    // done!
    spark.stop()
  }
}

/*
spark=org.apache.spark.sql.SparkSession@b34832b
inputPath=data/sample_numbers.txt
rdd=data/sample_numbers.txt MapPartitionsRDD[1] at textFile at MinMaxForceEmptyPartitions.scala:162
rdd.count=11
rdd.collect()=Array(23,24,22,44,66,77,44,44,555,666, 12,4,555,66,67,68,57,55,56,45,45,45,66,77, 34,35,36,97300,78,79, 120,44,444,445,345,345,555, 11,33,34,35,36,37,47,7777,8888,6666,44,55, 10,11,44,66,77,78,79,80,90,98,99,100,102,103,104,105, 6,7,8,9,10, 8,9,10,12,12, 7777, 222,333,444,555,666,111,112,5,113,114, 5555,4444,24)
rdd.getNumPartitions()=17
minMaxCount=MapPartitionsRDD[2] at mapPartitions at MinMaxForceEmptyPartitions.scala:174
firstRecord=23,24,22,44,66,77,44,44,555,666
firstRecord=120,44,444,445,345,345,555
firstRecord=11,33,34,35,36,37,47,7777,8888,6666,44,55
firstRecord=12,4,555,66,67,68,57,55,56,45,45,45,66,77
firstRecord=34,35,36,97300,78,79
firstRecord=10,11,44,66,77,78,79,80,90,98,99,100,102,103,104,105
firstRecord=6,7,8,9,10
firstRecord=7777
firstRecord=5555,4444,24
minMaxCount.count=17
firstRecord=120,44,444,445,345,345,555
firstRecord=23,24,22,44,66,77,44,44,555,666
firstRecord=12,4,555,66,67,68,57,55,56,45,45,45,66,77
firstRecord=34,35,36,97300,78,79
firstRecord=11,33,34,35,36,37,47,7777,8888,6666,44,55
firstRecord=10,11,44,66,77,78,79,80,90,98,99,100,102,103,104,105
firstRecord=5555,4444,24
firstRecord=6,7,8,9,10
firstRecord=7777
minMaxCount.collect()=Array((22,666,10), (4,555,14), (1,-1,0), (1,-1,0), (34,97300,6), (44,555,7), (11,8888,12), (1,-1,0), (1,-1,0), (10,105,16), (1,-1,0), (1,-1,0), (6,12,10), (5,7777,11), (1,-1,0), (24,5555,3), (1,-1,0))
final: (min, max, count)= (4, 97300, 89)
 */
