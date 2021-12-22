from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
import json

#-----------------------------------------------------
# Write Content of an RDD to an ElasticSearch
# Input: ElasticSearch Hostname
#------------------------------------------------------
# Input Parameters:
#    ElasticSearch Hostname
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------

#=============================================
def format_data(data):
    return (data['doc_id'], json.dumps(data))
#end-def
#=============================================
def main():

    if len(sys.argv) != 2:  
        print("Usage: datasource_elasticsearch_writer.py <es-hostname>", file=sys.stderr)
        exit(-1)

    # read name of ElasticSearch Hostname
    es_hostname = sys.argv[1]
    print("es_hostname : ", es_hostname)

    # create an instance of SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Write to ElasticSearch

    # To write an RDD to Elasticsearch you need to 
    # first specify a configuration. This includes the 
    # location of the Elasticsearch cluster's Master 
    # Node, it's port, and the index and document type 
    # you are writing to. There are numerous things that 
    # can be configured prior to writing to Elasticsearch 
    # but we’ll save those for later. Your conf should look 
    # something like:
    #
    es_write_conf = {
        # specify the node that we are sending data to 
        # (this should be the master)
        "es.nodes" : es_hostname,
        # specify the port in case it is not the default port
        "es.port" : '9200',
        # specify a resource in the form 'index/doc-type'
        "es.resource" : 'testindex/testdoc',
        # is the input JSON?
        "es.input.json" : "yes",
        # is there a field in the mapping that should be 
        # used to specify the ES document ID
        "es.mapping.id": "doc_id"
    }
    
    # PREPPING THE DATA for Writing to ES
 
    # Now, we need to ensure that our RDD has 
    # records of the type:
    #
    #   (0, "{'some_key': 'some_value', 'doc_id': 123}")
    #
    # Note that we have an RDD of tuples. The first value 
    # in the tuple (the key) actually doesn’t matter. 
    # elasticsearch-hadoop only cares about the value. You 
    # need to ensure that the value is a valid JSON string. 
    # So, you could try starting with data like:
    #
    data = [
        {'key1': 'some_value1', 'doc_id': 100},
        {'key2': 'some_value2', 'doc_id': 200},
        {'key3': 'some_value3', 'doc_id': 300},
        {'key4': 'some_value4', 'doc_id': 400}     
    ]
    #
    print("data : \n",  data)
    
    # Now we can create an RDD from this:
    rdd = spark.sparkContext.parallelize(data)
    print("rdd : ",  rdd)
    print("rdd.count() : ",  rdd.count())
    print("rdd.collect() \n: ",  rdd.collect())
    
    # Now we can define a function to format this 
    # into an elasticsearch-hadoop compatible RDD:
    # def format_data(x):
    #    return (x['doc_id'], json.dumps(x))

    rdd_formatted = rdd.map(lambda x: format_data(x))
    print("rdd_formatted.count() : ",  rdd_formatted.count())
    print("rdd_formatted.collect() \n: ",  rdd_formatted.collect())
 
    # EXECUTING THE WRITE OPERATION
    # Now we can write rdd_formatted to ES.
    # You just need to execute the following 
    # action on your RDD to write it to Elasticsearch:
    rdd_formatted.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        # critically, we must specify our `es_write_conf`
        conf=es_write_conf
    ) 
 
    # done!
    spark.stop()
#end-def
#=============================================

if __name__ == '__main__':
    main()