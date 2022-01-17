package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
/**
 *-----------------------------------------------------
 * Read Content of MongoDB to Dataframe
 * Input: MongoDB Collection uri (mongodb://127.0.0.1/test.myCollection)
 *------------------------------------------------------
 * Input Parameters:
 *    MongoDB Collection uri (mongodb://127.0.0.1/test.myCollection)
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object DatasourceMongodbReader {
  def main(args: Array[String]): Unit = {

    //read name of mongodb collection URI
    val mongoDBCollectionURI = args(0)
    println(s"MongoDBCollectionURI = ${mongoDBCollectionURI}")
    //create an instance of SparkSession
    val spark =
      SparkSession.
        builder().
        appName("DatasourceMongodbReader").
        master("local[*]").
        config("spark.mongodb.input.uri",mongoDBCollectionURI).
        config("spark.mongodb.output.uri",mongoDBCollectionURI).
        getOrCreate()

    //Assign the collection to a DataFrame with spark.read()
    val columnNames = List("name", "city", "age")
    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load.select(columnNames.map(m=>col(m)):_*)
    //To read the contents of the DataFrame, use the show() method.
    df.show(10,truncate = false)
    println(s"df.count(): ${df.count()}")
    println(s"df.collect(): ${df.collect().mkString("",",","")}")
    //Spark samples the records to infer the schema of the collection.
    df.printSchema()

    //Done.
    spark.stop()

  }

}

/*
df.show
+-------+---------+---+
|name   |city     |age|
+-------+---------+---+
|Betty  |Ames     |78 |
|Gandalf|Cupertino|60 |
|Brian  |Stanford |77 |
|Alex   |Ames     |50 |
|Thorin |Sunnyvale|95 |
+-------+---------+---+
 */