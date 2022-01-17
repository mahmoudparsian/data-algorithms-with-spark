package org.data.algorithms.spark.ch07

import org.apache.spark.sql.SparkSession
/**
 *-----------------------------------------------------
 * Write Content of an Dataframe to MongoDB
 * Input: MongoDB Collection uri (mongodb://127.0.0.1/test.myCollection)
 *------------------------------------------------------
 * Input Parameters:
 *    MongoDB Collection uri (mongodb://127.0.0.1/test.myCollection)
 *-------------------------------------------------------
 *
 * @author Deepak Kumar
 *-------------------------------------------------------
 */
object DatasourceMongodbWriter {
  def main(args: Array[String]): Unit = {

    //read name of mongodb collection URI
    val mongoDBCollectionURI = args(0)
    println(s"MongoDBCollectionURI = ${mongoDBCollectionURI}")
    //create an instance of SparkSession
    val spark =
      SparkSession.
        builder().
        appName("DatasourceMongodbWriter").
        master("local[*]").
        config("spark.mongodb.input.uri",mongoDBCollectionURI).
        config("spark.mongodb.output.uri",mongoDBCollectionURI).
        getOrCreate()

    /*
    # Write to MongoDB
    # Use the SparkSession.createDataFrame() function to create a DataFrame.
    # In the following example, createDataFrame() takes a list of tuples containing
    # names, cities, and ages, and a list of column names:
    */
    val columnNames = List("name", "city", "age")
    val listOfValues = List(
      ("Alex", "Ames", 50),
      ("Gandalf", "Cupertino", 60),
      ("Thorin", "Sunnyvale", 95),
      ("Betty", "Ames", 78),
      ("Brian", "Stanford", 77)
    )
    val people = spark.createDataFrame(listOfValues).toDF(columnNames:_*)
    /*
    # Write the people DataFrame to the MongoDB database and
    # collection specified in the spark.mongodb.output.uri
    # option by using the write method:
    #
    # The following operation writes to the MongoDB database and
    # collection specified in the spark.mongodb.output.uri option.
    */
    people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
    //To read the contents of the DataFrame, use the show() method.
    people.show(10,truncate = false)
    println(s"df.count(): ${people.count()}")
    println(s"df.collect(): ${people.collect().mkString("",",","")}")
    //Spark samples the records to infer the schema of the collection.
    people.printSchema()

    //Done.
    spark.stop()

  }

}

/*
>db.myCollection.find({name:"Alex"})
{ "_id" : ObjectId("61e2c446027d5b4b66ca6555"), "name" : "Alex", "city" : "Ames", "age" : 50 }
 */