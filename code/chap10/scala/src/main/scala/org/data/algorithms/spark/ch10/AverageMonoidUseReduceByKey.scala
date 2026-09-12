package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

/**
 * <1> Make sure that we have 1 parameters in the command line
 * <2> Create an instance of a SparkSession object by using the builder pattern SparkSession.builder class
 * <3> Define input path (this can be a file or a directory containing any number of files
 * <4> Read input and create the first RDD as RDD[String] where each object has this foramt: "key,number"
 * <5> Create key_number_one RDD as (key, (number, 1))
 * <6> Aggregate (sum1, count1) with (sum2, count2) and create (sum1+sum2, count1+count2) as values
 * <7> Apply the mapValues() transformation to find final average per key
 */
object AverageMonoidUseReduceByKey {

  /**
   *------------------------------
   * function: createPair() to accept
   * a String object as "key,number" and
   * returns a (key, number) pair.
   *
   * record as String of "key,number"
   */
  def createPair(record: String): (String, (Int, Int)) = {
    val tokens = record.split(",")
    //key -> tokens(0) as String
    //number -> tokens(1) as Integer
    (tokens(0), (tokens(1).toInt, 1))
  }

  /**
   *------------------------------------
   * function:  `addPairs` accept two
   * tuples of (sum1, count1) and (sum2, count2)
   * and returns sum of tuples (sum1+sum2, count1+count2).
   *
   * a = (sum1, count1)
   * b = (sum2, count2)
   */
  def addPairs(a: (Int, Int), b:(Int, Int)): (Int, Int) = {
    // sum = sum1+sum2
    val sum = a._1 + b._1
    // count = count1+count2
    val count = a._2 + b._2
    (sum, count)
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

    // create a pair of (key, (number, 1)) for "key,number"
    // <5>
    val sumAndFreq = records.map(createPair)
    print("sumAndFreq.count(): " + sumAndFreq.count())
    print("sumAndFreq.collect(): " + sumAndFreq.collect().mkString("Array(", ", ", ")"))

    /**
     *------------------------------------------------------------
     * aggregate the (sum, count) of each unique key
     * <6>
     */
    val sumCount = sumAndFreq.reduceByKey(addPairs)
    println("sumCount.count(): " + sumCount.count())
    println("sumCount.collect(): " + sumCount.collect().mkString("Array(", ", ", ")"))

    /**
     * create the final RDD as RDD[key, average]
     * <7>
     * v = (v[0], v[1]) = (sum, count)
     */
    val averages =  sumCount.mapValues(v => v._1.toFloat / v._2.toFloat)
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
