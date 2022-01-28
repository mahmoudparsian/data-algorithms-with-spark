package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Try

/*
-----------------------------------------------------
 Find Top-N per key by using mapPartitions().
 The idea is that each partition will find Top-N,
 and then we find Top-N for all partitions.

 input ---- partitioned ---->  partition-1, partition-2, ...

 partition-1 => local-1-Top-N
 partition-2 => local-2-Top-N
 ...

 final-top-N = Top-N(local-1-Top-N, local-2-Top-N, ...)

------------------------------------------------------
 Input Parameters:
      N
  -------------------------------------------------------
   @author Deepak Kumar
  -------------------------------------------------------
*/
object TopNUseMapPartitions {

  def debugPartition(iterator: Iterator[(String, Int)]) : Unit = {
    println("===begin-partition===")
    for(x<-iterator){
      println(x)
    }
    println("===end-partition===")
  }
  /*
  ==========================================
   Find Top-N for a given single partition.

   partition_iterator is an iterator over
   elements of a single partition.
   partition_iterator : iterator over
   [(key1, number1), (key2, number2), ...]
  ==========================================
  */
  def top(partitionIterator: Iterator[(String, Int)], N: Int): Iterator[(String, Int)] = {
    val sm = mutable.SortedMap.empty[Int, String]
    for ((key, number) <- partitionIterator) {
      //add an entry of (key, number) at s
      sm += number -> key
      if(sm.size > N)
        {
          sm -= sm.firstKey
        }

    }
    val pairs = for((k,v) <- sm)
      yield (v,k)
    pairs.toList.iterator
  }

  def main(args: Array[String]): Unit = {
    if(args.length !=1){
      println("Usage: TopNUseMapPartitions <N>")
      sys.exit(-1)
    }
    //create an instance of SparkSession
    val spark = SparkSession
      .builder()
      .appName("TopNUseMapPartitions")
      .master("local[*]")
      .getOrCreate()
    println(s"spark =$spark")
    //Top-N
    val N = Try(args(0).toInt) getOrElse 1
    println(s"N: $N")
    /*
    =====================================
    * Create an RDD from a list of values
    =====================================
    */
    val listOfKeys = List(
      ("a", 1), ("a", 7), ("a", 2), ("a", 3),
      ("b", 2), ("b", 4),
      ("c", 10), ("c", 50), ("c", 60), ("c", 70),
      ("d", 5), ("d", 15), ("d", 25),
      ("e", 1), ("e", 2),
      ("f", 9), ("f", 2),
      ("g", 22), ("g", 12),
      ("h", 3), ("h", 4), ("h", 5), ("h", 6),
      ("i", 30), ("i", 40),
      ("j", 50), ("j", 60),
      ("k", 30)
    )
    println("listOfKeys = %s".format(listOfKeys))
    /*
    *=====================================
    * create an RDD from a collection
    * Distribute a local Scala collection to form an RDD
    *=====================================
    */
    val rdd = spark.sparkContext.parallelize(listOfKeys)
    println("rdd= %s".format(rdd))
    println(s"rdd.count= ${rdd.count()}")
    println(s"rdd.collect()= ${rdd.collect().mkString("Array(", ", ", ")")}")
    /*
    *=====================================
    * Make sure same keys are combined and then partition it into "num_of_partitions" partitions:
    * Here I set it to 2: (for debugging purposes):
    *====================================
    */
    val numOfPartitions = 2
    val combined = rdd.reduceByKey((a,b) => a+b).coalesce(numOfPartitions)
    println("combined= %s".format(combined))
    println(s"combined.count= ${combined.count()}")
    println(s"combined.collect()= ${combined.collect().mkString("Array(", ", ", ")")}")
    /*
    =====================================
    for debugging purposes:
    check content of each partition:
    */
    combined.foreachPartition(debugPartition)
    /*
    =====================================
     find local Top-N's per partition
    =====================================
    x = (key, number)
    x[0] --> key
    x[1] --> number
    */
    val topN = combined.mapPartitions(partition => top(partition,N))
    println("topN= %s".format(topN))
    println(s"topN.count= ${topN.count()}")
    println(s"topN.collect()= ${topN.collect().mkString("Array(", ", ", ")")}")
    /*
    #=====================================
    # find final Top-N from all partitions
      #=====================================
    */
    val finalTopN = top(topN.collect().iterator, N)
    print(s"final_topN = ${finalTopN.toList.mkString("(",",",")")}")
    //done!
    spark.stop()
  }

}
