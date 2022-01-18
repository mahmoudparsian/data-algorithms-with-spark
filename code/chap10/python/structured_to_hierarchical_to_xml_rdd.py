#-----------------------------------------------------
# This is an example of  "Structured to Hierarchical" 
# Pattern in PySpark.
# 
# RRD-based solution
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
def create_xml(element):
    #
    post_id = element[0]
    iterables = element[1]
    FIRST_TIME = True
    #
    xml_string = "<post id=\"" + post_id + "\">"
    for t2 in iterables:
        title, creator = t2[0]
        comment, commented_by = t2[1]
        if (FIRST_TIME):
            xml_string += "<title>" + title + "</title>"
            xml_string += "<creator>" + creator + "</creator>"
            FIRST_TIME = False
        #end-if
        xml_string += "<comment>" + comment + "</comment>"
    #end-for
    xml_string += "</post>" 

    #end-for
    return xml_string
#end-def
#-----------------------------------
def main():

    # create an instance of SparkSession object
    spark = SparkSession.builder.getOrCreate()

    posts_data = [('p1', ('t1', 'creator1')), ('p2', ('t2', 'creator2')), ('p3', ('t3', 'creator3'))]

    comments_data =  [('p1', ('comment-11', 'by-11')), \
                      ('p1', ('comment-12', 'by-12')), \
                      ('p1', ('comment-13', 'by-13')), \
                      ('p2', ('comment-21', 'by-21')), \
                      ('p2', ('comment-22', 'by-22')), \
                      ('p2', ('comment-23', 'by-23')), \
                      ('p2', ('comment-24', 'by-24')), \
                      ('p3', ('comment-31', 'by-31')), \
                      ('p3', ('comment-32', 'by-32'))] 
       
    posts = spark.sparkContext.parallelize(posts_data)
    print("posts.count() : ", posts.count())
    print("posts.collect() : ", posts.collect())
    
    comments = spark.sparkContext.parallelize(comments_data)
    print("comments.count() : ", comments.count())
    print("comments.collect() : ", comments.collect())
    
    joined = posts.join(comments)

    grouped = joined.groupByKey()
    print("grouped.count() : ", grouped.count())
    print("grouped.collect() : ", grouped.mapValues(lambda vs : list(vs)).collect())

    xml_rdd = grouped.map(create_xml)
    print("xml_rdd.count() : ", xml_rdd.count())
    print("xml_rdd.collect() : ", xml_rdd.collect())
        
    spark.stop()
#end-def
#-----------------------------------

if __name__ == "__main__":
    main()
    
"""
sample run:

$SPARK_HOME/bin/spark-submit structured_to_hierarchical_to_xml_rdd.py

posts.count() :  3
posts.collect() :  
[
 ('p1', ('t1', 'creator1')), 
 ('p2', ('t2', 'creator2')), 
 ('p3', ('t3', 'creator3'))
]

comments.count() :  9
comments.collect() :  
[ ('p1', ('comment-11', 'by-11')), 
  ('p1', ('comment-12', 'by-12')), 
  ('p1', ('comment-13', 'by-13')), 
  ('p2', ('comment-21', 'by-21')), 
  ('p2', ('comment-22', 'by-22')), 
  ('p2', ('comment-23', 'by-23')), 
  ('p2', ('comment-24', 'by-24')), 
  ('p3', ('comment-31', 'by-31')), 
  ('p3', ('comment-32', 'by-32'))
]

grouped.count() :  3
grouped.collect() :  
[
 ('p1', [(('t1', 'creator1'), ('comment-11', 'by-11')), 
         (('t1', 'creator1'), ('comment-12', 'by-12')), 
         (('t1', 'creator1'), ('comment-13', 'by-13'))]
 ), 
 ('p2', [(('t2', 'creator2'), ('comment-21', 'by-21')), 
         (('t2', 'creator2'), ('comment-22', 'by-22')), 
         (('t2', 'creator2'), ('comment-23', 'by-23')), 
         (('t2', 'creator2'), ('comment-24', 'by-24'))]
 ), 
 ('p3', [(('t3', 'creator3'), ('comment-31', 'by-31')), 
         (('t3', 'creator3'), ('comment-32', 'by-32'))]
 )
]

xml_rdd.count() :  3
xml_rdd.collect() :  
[
 '<post id="p1"><title>t1</title><creator>creator1</creator><comment>comment-11</comment><comment>comment-12</comment><comment>comment-13</comment></post>', 
 '<post id="p2"><title>t2</title><creator>creator2</creator><comment>comment-21</comment><comment>comment-22</comment><comment>comment-23</comment><comment>comment-24</comment></post>', 
 '<post id="p3"><title>t3</title><creator>creator3</creator><comment>comment-31</comment><comment>comment-32</comment></post>'
]


"""