package org.data.algorithms.spark.ch10

import org.apache.spark.sql.{SparkSession, functions}

/*-----------------------------------------------------
 * This is an example of  "Structured to Hierarchical" Pattern in PySpark.
 *
 * Dataframe-based solution
 * ------------------------------------------------------
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *------------------------------------------------------
 * Input Parameters: none
 *-------------------------------------------------------
 * @author Deepak Kumar
 *-------------------------------------------------------

*/
object StructuredToHierarchicalToXmlDataframe {
  /*
  -----------------------------------
   element: (post_id, Iterable<((title, creator),(comment, commented_by))>)
    
   creates xml as:
  	<post id="post_id">
    		<title>t1</title>
    		<creator>creator1</creator>
    		<comments>
      			<comment>comment-11</comment>
      			<comment>comment-12</comment>
      			<comment>comment-13</comment>
      		</comments>
    	</post>
  */
  def createXML(post_id: String, title: String, creator: String, comments: Array[String]): String = {
    var xmlString = "<post id=\"" + post_id + "\">"
    xmlString += "<title>" + title + "</title>"
    xmlString += "<creator>" + creator + "</creator>"
    for(comment <- comments){
      xmlString += "<comment>" + comment + "</comment>"
    }
    xmlString += "</post>"
    return xmlString
  }


  def main(args: Array[String]): Unit = {
    //create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val postsData = List(("p1", "t1", "creator1"), ("p2", "t2", "creator2"), ("p3", "t3", "creator3"))
    val commentsData = List(
      ("p1", "comment-11", "by-11"),
      ("p1", "comment-12", "by-12"),
      ("p1", "comment-13", "by-13"),
      ("p2", "comment-21", "by-21"),
      ("p2", "comment-22", "by-22"),
      ("p2", "comment-23", "by-23"),
      ("p2", "comment-24", "by-24"),
      ("p3", "comment-31", "by-31"),
      ("p3", "comment-32", "by-32")
    )
    //val postsSchema = List(StructField("post_id", StringType, true), StructField("title", StringType, true),StructField("title", StringType, true))
    val postsColumns: Seq[String] = Seq("post_id","title","creator")
    val posts = spark.createDataFrame(postsData).toDF(postsColumns:_*)
    println(s"posts.count() : ${posts.count()}")
    posts.show(truncate=false)

    val commentColumns: Seq[String] = Seq("post_id","comment","commented_by")
    val comments = spark.createDataFrame(commentsData).toDF(commentColumns:_*)

    println(s"posts.count() : ${posts.count()}")
    posts.show(truncate=false)

    val joinedAndSelected = posts.join(comments , posts("post_id") === comments("post_id")).
      select(posts("post_id"),posts("title"),posts("creator"),comments("comment"))
    joinedAndSelected.show(truncate = false)

    val grouped = joinedAndSelected.groupBy("post_id", "title", "creator").agg(functions.collect_list("comment").alias("comments"))
    grouped.show(truncate = false)

    val createXMLUdf = functions.udf((post_id, title, creator, comments) => createXML(post_id, title, creator, comments))
    val df = grouped.withColumn("xml", createXMLUdf(grouped("post_id"), grouped("title"), grouped("creator"), grouped("comments")))
      .drop("title")
      .drop("creator")
      .drop("comments")
    df.show(truncate=false)

    spark.stop()

  }
}
