package org.data.algorithms.spark.ch07

import org.apache.spark.sql.{SaveMode, SparkSession}
/**
 *-----------------------------------------------------
 * Write Content of an Dataframe to Redis
 * Input: Redis Hostname Redis Port
 *------------------------------------------------------
 * Input Parameters:
 *    Redis Hostname Redis Port
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object DatasourceRedisWriter {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: DatasourceRedisWriter <redis-host> <redis-port>")
      System.exit(-1)
    }

    //redis host
    val REDIS_HOST = args(0)
    println(s"REDIS_HOST = ${REDIS_HOST}")
    //redis port number
    val REDIS_PORT = args(1)
    print("REDIS_PORT = ", REDIS_PORT)
    //create an instance of SparkSession
    val spark =
      SparkSession.
        builder().
        master("local[*]").
        config("spark.redis.host",REDIS_HOST).
        config("spark.redis.port",REDIS_PORT).
        getOrCreate()
    //
    // you may add the password, if desired
    //     .config("spark.redis.auth", "passwd")
    //
    /*
    ========================================
     Write to Redis from a DataFrame
    ========================================


     Use the SparkSession.createDataFrame() function
     to create a DataFrame. In the following example,
     createDataFrame() takes a list of tuples containing
     names, cities, and ages, and a list of column names:
    */
    val columnNames = List("name", "city", "age")
    val listOfValues = List(
      ("Alex", "Ames", 50),
      ("Gandalf", "Cupertino", 60),
      ("Thorin", "Sunnyvale", 95),
      ("Betty", "Ames", 78),
      ("Brian", "Stanford", 77)
    )
    val df = spark.createDataFrame(listOfValues).toDF(columnNames:_*)
    println(s"df.count(): ${df.count()}")
    println(s"df.collect(): ${df.collect().mkString("",",","")}")
    df.show()
    df.printSchema()

    //-----------------------------------
    // Write an existing DataFrame (df)
    // to a redis database:
    //-----------------------------------
    df.write
      .mode(SaveMode.Overwrite)
      .format("org.apache.spark.sql.redis")
      .option("table","people")
      .option("key.column","name")
      .save()
    //-----------------------------------
    // READ back data from Redis:
    //-----------------------------------
    val loadedDF = spark.read
    .format("org.apache.spark.sql.redis")
    .option("table", "people")
    .option("key.column", "name")
    .load()

    println(s"loadedDF.count(): ${loadedDF.count()}")
    println(s"loadedDF.collect(): ${loadedDF.collect().mkString("",",","")}")
    loadedDF.show()
    loadedDF.printSchema()

    //Done.
    spark.stop()

  }

}
