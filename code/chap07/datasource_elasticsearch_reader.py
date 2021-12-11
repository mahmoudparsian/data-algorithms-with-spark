from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
import json

#-----------------------------------------------------
# Read From ElasticSearch and write to an RDD
# Input: ElasticSearch Hostname
#------------------------------------------------------
# Input Parameters:
#    ElasticSearch Hostname
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
def main():

    if len(sys.argv) != 2:  
        print("Usage: datasource_elasticsearch_reader.py <es-hostname>", file=sys.stderr)
        exit(-1)

    # read name of ElasticSearch Hostname
    es_hostname = sys.argv[1]
    print("es_hostname : ", es_hostname)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()   
    
    # Read from ElasticSearch and Write to RDD

    # Now that we have some data in Elasticsearch
    # (by running datasource_elasticsearch_writer.py), 
    # we can read it back in using the elasticsearc-hadoop 
    # connector.
    #
    es_read_conf = {
        # specify the node that we are sending data to 
        # (this should be the master node)
        "es.nodes" : es_hostname,
        # specify the port in case it is not the default port
        "es.port" : '9200',
        # specify a resource in the form 'index/doc-type'
        "es.resource" : 'testindex/testdoc'
    }
    
    # Now we can generate an RDD from this Elasticsearch data:
    es_rdd = spark.sparkContext.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf
    )
    #
    print("rs_rdd : ",  es_rdd)
    print("es_rdd.count() : ",  es_rdd.count())
    print("es_rdd.collect() \n: ", es_rdd.collect())
    
    # done!
    spark.stop()
#end-def

if __name__ == '__main__':
    main()

