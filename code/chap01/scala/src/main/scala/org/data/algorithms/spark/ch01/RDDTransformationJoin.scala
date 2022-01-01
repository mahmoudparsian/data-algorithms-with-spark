package org.data.algorithms.spark.ch01

import org.apache.spark.sql.SparkSession

/**
 *-------------------------------------------------------
 * Apply a join() transformation to an RDD
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object RDDTransformationJoin {

  def main(args: Array[String]): Unit = {

    // create an instance of SparkSession
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /**
     * ----------------------------------------------------
     * join() transformation
     *
     * join(other)
     *
     * Description:
     * Return an RDD containing all pairs of elements with
     * matching keys in self and other.  Each pair of elements
     * will be returned as a (k, (v1, v2)) tuple, where (k, v1)
     * is in self RDD and (k, v2) is in other RDD. Performs a
     * hash join across the cluster.
     *
     * ----------------------------------------------------
     */
    val sourcePairs = List((1,"u"), (1,"v"), (2, "a"), (3,"b"), (4,"z1"))
    println("sourcePairs = " + sourcePairs)
    val source = spark.sparkContext.parallelize(sourcePairs)
    println("source.count(): " + source.count())
    println("source.collect(): " + source.collect().mkString("Array(", ", ", ")"))
    val otherPairs = List((1,"x"), (1,"y"), (2, "c"), (2,"d"), (3,"m"), (8,"z2"))
    println("otherPairs = " + otherPairs)
    val other = spark.sparkContext.parallelize(otherPairs)
    println("other.count(): " + other.count())
    println("other.collect(): " + other.collect().mkString("Array(", ", ", ")"))

    /**
     *-----------------------------------------
     * source.join(other)
     *-----------------------------------------
     */
    val joined = source.join(other)
    println("joined.count(): " + joined.count())
    println("joined.collect(): " + joined.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()

  }

}
