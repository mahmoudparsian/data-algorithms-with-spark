package org.data.algorithms.spark.ch10

import org.apache.spark.sql.SparkSession

/*-----------------------------------------------------
 * This is an example of  "Structured to Hierarchical" Pattern in PySpark.
 *
 * RRD-based solution
 * ------------------------------------------------------
 * NOTE: print() and collect() are used for debugging and educational purposes.
 *------------------------------------------------------
 * Input Parameters: none
 *-------------------------------------------------------
 * @author Deepak Kumar
 *-------------------------------------------------------

*/
object StructuredToHierarchicalToXmlRDD {
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
  def createXML(element: (String, Iterable[((String, String), (String, String))])): String = {
    val postID = element._1
    val iterables = element._2
    var FIRST_TIME = true
    var xmlString = "<post id=\"" + postID + "\">"
    for(t2 <- iterables){
      val (title , creator) = (t2._1._1 , t2._1._2)
      val (comment , commentedBy) = (t2._2._1,t2._2._2)
      if(FIRST_TIME){
        xmlString += "<title>" + title + "</title>"
        xmlString += "<creator>" + creator + "</creator>"
        FIRST_TIME = false
      }
      xmlString += "<comment>" + comment + "</comment>"
    }
    xmlString += "</post>"
    return xmlString
  }

  def main(args: Array[String]): Unit = {
    //create an instance of SparkSession object
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val postsData = List(("p1", ("t1", "creator1")), ("p2", ("t2", "creator2")), ("p3", ("t3", "creator3")))
    val commentsData = List(
      ("p1", ("comment-11", "by-11")),
      ("p1", ("comment-12", "by-12")),
      ("p1", ("comment-13", "by-13")),
      ("p2", ("comment-21", "by-21")),
      ("p2", ("comment-22", "by-22")),
      ("p2", ("comment-23", "by-23")),
      ("p2", ("comment-24", "by-24")),
      ("p3", ("comment-31", "by-31")),
      ("p3", ("comment-32", "by-32"))
    )
    val posts = spark.sparkContext.parallelize(postsData)
    println(s"posts.count() : ${posts.count()}")
    println(s"posts.collect() : ${posts.collect().mkString("Array(", ", ", ")")}")
    val comments = spark.sparkContext.parallelize(commentsData)
    println(s"posts.count() : ${posts.count()}")
    println(s"posts.collect() : ${posts.collect().mkString("Array(", ", ", ")")}")
    val joined = posts.join(comments)
    val grouped = joined.groupByKey()
    println(s"grouped.count() : ${grouped.count()}")
    println(s"grouped.collect() : ${grouped.mapValues(vs => vs.toList).collect().mkString("Array(", ", ", ")")}")
    val xmlRDD = grouped.map(createXML)
    println(s"xmlRDD.count() : ${xmlRDD.count()}")
    println(s"xmlRDD.collect() : ${xmlRDD.collect().mkString("Array(", ", ", ")")}")

  }
}
