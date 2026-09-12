package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Create a Redis (key, value) pairs from an existing DataFrame
 *
 * Input Parameters:
 *       REDIS_HOST
 *       REDIS_PORT
 *------------------------------------------------------
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceRedisReader {

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      System.err.println("Pass Redis Server and Port in Command Line Argument")
      System.exit(-1)
    }

    // redis host
    val REDIS_HOST = args(0)
    println("REDIS_HOST = " + REDIS_HOST)
    //
    // redis port number
    val REDIS_PORT = args(1)
    println("REDIS_PORT = " + REDIS_PORT)


    // create an instance of SparkSession
    val spark = SparkSession
      .builder()
      .appName("datasource redis reader")
      .config("spark.redis.host", REDIS_HOST)
      .config("spark.redis.port", REDIS_PORT)
      .master("local[*]")
      .getOrCreate()

    // you may add the password, if desired
    //     .config("spark.redis.auth", "passwd")
    println("spark = " +  spark)


    /**
     *========================================
     * Read from Redis
     *========================================
     */
    val loadedDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "people")
      .option("key.column", "name")
      .option("infer.schema", "true")
      .load()

    println("loadedDf = " + loadedDf)
    println("loadedDf.count(): " + loadedDf.count())
    println("loadedDf.collect(): " + loadedDf.collect().mkString("Array(", ", ", ")"))
    loadedDf.show()
    loadedDf.printSchema()

    // done!
    spark.stop()
  }

}
