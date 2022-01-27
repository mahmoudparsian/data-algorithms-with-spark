package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

/**
 * <1> Make sure that we have 1 parameters in the command line
 * <2> Create an instance of a SparkSession object by using the builder pattern SparkSession.builder class
 * <3> Define input path (this can be a file or a directory containing any number of files
 * <4> Read input and create the first RDD as RDD[String] where each object has this foramt: "key,number"
 * <5> Create (key, value) pairs RDD as (key, number)
 * <6> groupByKey() and create [(key1, [v11, v12, ...]), (key2, [v21, v22, ...]), ...]
 * <7> Apply the mapValues() transformation to find final average per key
 */

/**
 *------------------------------------------
 * NOTE:
 *
 * In general, avoid using groupByKey(), and
 * instead use reduceByKey() or combineByKey().
 * For details see:
 *   https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
 *
 * The groupByKey() solution is provided for educational
 * purposes.  If you need all of the values of a key for
 * some aggregation such as finding the "median" (which you
 * need all of the values per key), then  the groupByKey()
 * may be used.
 */
object AverageMonoidUseGroupByKey {

  /**
   *------------------------------
   * function: createPair() to accept
   * a String object as "key,number" and
   * returns a (key, number) pair.
   *
   * record as String of "key,number"
   */
  def createPair(record: String): (String, Int) = {
    val tokens = record.split(",")
    //key -> tokens(0) as String
    //number -> tokens(1) as Integer
    (tokens(0), tokens(1).toInt)
  }

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    // <1>
    if (args.length != 1) {
      System.err.println("Input Path is missing in Sytem Arguments")
      System.exit(-1)
    }

    // <2>
    val spark = createSparkSession()

    //  args(0) is the first parameter
    // <3>
    val inputPath = args(0)
    println(s"inputPath: $inputPath")

    // read input and create an RDD<String>
    // <4>
    val records = spark.sparkContext.textFile(inputPath)
    println("records.count(): " + records.count())
    println("records.collect(): " + records.collect().mkString("Array(", ", ", ")"))

    // create a pair of (key, number) for "key,number"
    // <5>
    val pairs = records.map(createPair)
    println("pairs.count(): " + pairs.count())
    println("pairs.collect(): " + pairs.collect().mkString("Array(", ", ", ")"))

    // aggregate the (sum, count) of each unique key
    // <6>
    val groupedByKey = pairs.groupByKey()
    println("groupedByKey : " + groupedByKey)
    println("groupedByKey.count(): " + groupedByKey.count())
    println("groupedByKey.collect(): " + groupedByKey.collect().mkString("Array(", ", ", ")"))
    println("groupedByKey.mapValues(lambda values : list(values)).collect(): "
      + groupedByKey.mapValues(List(_)).collect().mkString("Array(", ", ", ")"))

    // create the final RDD as RDD[key, average]
    // <7>
    // values = (number1, number2, number3, ...)
    val averages =  groupedByKey.mapValues(values => values.sum.toFloat / values.size.toFloat)
    println("averages.count(): " + averages.count())
    println("averages.collect(): " + averages.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()
  }
}

/*
inputPath: data/sample_input.txt
records.count(): 12
records.collect(): Array(a,2, a,3, a,4, a,5, a,7, b,4, b,5, b,6, c,3, c,4, c,5, c,6)
pairs.count(): 12
pairs.collect(): Array((a,2), (a,3), (a,4), (a,5), (a,7), (b,4), (b,5), (b,6), (c,3), (c,4), (c,5), (c,6))
sumCount.count(): 3
sumCount.collect(): Array((b,(15,3)), (a,(21,5)), (c,(18,4)))
averages.count(): 3
averages.collect(): Array((b,5.0), (a,4.2), (c,4.5))
 */
