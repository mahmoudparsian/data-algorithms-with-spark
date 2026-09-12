#-----------------------------------------------------
# This is an example of  "Structured to Hierarchical" 
# Pattern in PySpark.
# 
# DataFrame-based solution
#------------------------------------------------------
# NOTE: print() and collect() are used for
#       debugging and educational purposes.
#------------------------------------------------------
# Input Parameters: none
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

#-----------------------------------
# element: (post_id, Iterable<((title, creator),(comment, commented_by))>)
#
# creates xml as:
#	<post id="post_id">
#		<title>t1</title>
#		<creator>creator1</creator>
#		<comments>
#			<comment>comment-11</comment>
#			<comment>comment-12</comment>
#			<comment>comment-13</comment>
#		</comments>
#	</post>
#
def create_xml(post_id, title, creator, comments):
    #
    xml_string = "<post id=\"" + post_id + "\">"
    xml_string += "<title>" + title + "</title>"
    xml_string += "<creator>" + creator + "</creator>"    
    for comment in comments:
        xml_string += "<comment>" + comment + "</comment>"
    #end-for
    xml_string += "</post>" 

    #end-for
    return xml_string
#end-def
#-----------------------------------

#-----------------------------------

def main():

    # create an instance of SparkSession object
    spark = SparkSession.builder.getOrCreate()

    posts_data = [('p1', 't1', 'creator1'), ('p2', 't2', 'creator2'), ('p3', 't3', 'creator3')]

    comments_data =  [('p1', 'comment-11', 'by-11'), \
                      ('p1', 'comment-12', 'by-12'), \
                      ('p1', 'comment-13', 'by-13'), \
                      ('p2', 'comment-21', 'by-21'), \
                      ('p2', 'comment-22', 'by-22'), \
                      ('p2', 'comment-23', 'by-23'), \
                      ('p2', 'comment-24', 'by-24'), \
                      ('p3', 'comment-31', 'by-31'), \
                      ('p3', 'comment-32', 'by-32')] 
       
    posts = spark.createDataFrame(posts_data, ["post_id", "title", "creator"])
    print("posts.count() : ", posts.count())
    posts.show(truncate=False)
    
    comments = spark.createDataFrame(comments_data, ["post_id", "comment", "commented_by"])
    print("comments.count() : ", comments.count())
    comments.show(truncate=False)
    
    joined_and_selected = posts.join(comments, posts.post_id == comments.post_id)\
        .select(posts.post_id, posts.title, posts.creator, comments.comment)
    print("joined_and_selected.count() : ", joined_and_selected.count())
    joined_and_selected.show(truncate=False)

    grouped = joined_and_selected.groupBy("post_id", "title", "creator").agg(F.collect_list("comment").alias("comments"))
    grouped.show(truncate=False)
    
    create_xml_udf = F.udf(lambda post_id, title, creator, comments: create_xml(post_id, title, creator, comments), StringType())

    df = grouped.withColumn("xml", create_xml_udf(grouped.post_id, grouped.title, grouped.creator, grouped.comments))\
      .drop("title")\
      .drop("creator")\
      .drop("comments")

    df.show(truncate=False)    
        
    spark.stop()
#end-def
#-----------------------------------

if __name__ == "__main__":
    main()
    
"""
sample run:

$SPARK_HOME/bin/spark-submit structured_to_hierarchical_to_xml_dataframe.py

posts.count() :  3
+-------+-----+--------+
|post_id|title|creator |
+-------+-----+--------+
|p1     |t1   |creator1|
|p2     |t2   |creator2|
|p3     |t3   |creator3|
+-------+-----+--------+

comments.count() :  9
+-------+----------+------------+
|post_id|comment   |commented_by|
+-------+----------+------------+
|p1     |comment-11|by-11       |
|p1     |comment-12|by-12       |
|p1     |comment-13|by-13       |
|p2     |comment-21|by-21       |
|p2     |comment-22|by-22       |
|p2     |comment-23|by-23       |
|p2     |comment-24|by-24       |
|p3     |comment-31|by-31       |
|p3     |comment-32|by-32       |
+-------+----------+------------+

joined_and_selected.count() :  9
+-------+-----+--------+----------+
|post_id|title|creator |comment   |
+-------+-----+--------+----------+
|p1     |t1   |creator1|comment-11|
|p1     |t1   |creator1|comment-12|
|p1     |t1   |creator1|comment-13|
|p2     |t2   |creator2|comment-21|
|p2     |t2   |creator2|comment-22|
|p2     |t2   |creator2|comment-23|
|p2     |t2   |creator2|comment-24|
|p3     |t3   |creator3|comment-31|
|p3     |t3   |creator3|comment-32|
+-------+-----+--------+----------+

+-------+-----+--------+------------------------------------------------+
|post_id|title|creator |comments                                        |
+-------+-----+--------+------------------------------------------------+
|p1     |t1   |creator1|[comment-11, comment-12, comment-13]            |
|p2     |t2   |creator2|[comment-21, comment-22, comment-23, comment-24]|
|p3     |t3   |creator3|[comment-31, comment-32]                        |
+-------+-----+--------+------------------------------------------------+

+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|post_id|xml                                                                                                                                                                                  |
+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|p1     |<post id="p1"><title>t1</title><creator>creator1</creator><comment>comment-11</comment><comment>comment-12</comment><comment>comment-13</comment></post>                             |
|p2     |<post id="p2"><title>t2</title><creator>creator2</creator><comment>comment-21</comment><comment>comment-22</comment><comment>comment-23</comment><comment>comment-24</comment></post>|
|p3     |<post id="p3"><title>t3</title><creator>creator3</creator><comment>comment-31</comment><comment>comment-32</comment></post>                                                          |
+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

"""