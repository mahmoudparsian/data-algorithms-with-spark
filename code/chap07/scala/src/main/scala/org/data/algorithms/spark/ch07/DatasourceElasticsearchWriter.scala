package org.data.algorithms.spark.ch07

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

/**
 *-----------------------------------------------------
 * Write Content of an RDD to an ElasticSearch
 * Input: ElasticSearch Hostname ElasticSearch Port
 *------------------------------------------------------
 * Input Parameters:
 *    ElasticSearch Hostname ElasticSearch Port
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceElasticsearchWriter {

  def formatData(jsonString: String): (String, JsonNode) = {
    val jsonNode = JsonUtils.toJsonObject(jsonString)
    (jsonNode.get("doc_id").asText(), jsonNode)
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      System.err.println("Pass Elasticsearch Server and Port in Command Line Argument")
      System.exit(-1)
    }

    // read name of ElasticSearch Hostname
    val esHostname = args(0)
    println("esHostname : " + esHostname)

    // read name of ElasticSearch Port
    val esPort = args(1)
    println("esPort : " + esPort)

    // create an instance of SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("mapred.map.tasks.speculative.execution", "false")
      .config("mapred.reduce.tasks.speculative.execution", "false")
      .getOrCreate()

    /**
     * Write to ElasticSearch
     *
     * To write an RDD to Elasticsearch you need to
     * first specify a configuration. This includes the
     * location of the Elasticsearch cluster's Master
     * Node, it's port, and the index and document type
     * you are writing to. There are numerous things that
     * can be configured prior to writing to Elasticsearch
     * but we’ll save those for later. Your conf should look
     * something like:
     *
     */
    // Now we can generate an RDD from this Elasticsearch data:
    val esWriteConf = new Configuration()
    // specify the node that we are sending data to
    // (this should be the master)
    esWriteConf.set("es.nodes", esHostname)
    // specify the port in case it is not the default port
    esWriteConf.set("es.port", esPort)
    // Comment it out incase your elastic search is not running in local docker
    esWriteConf.set("es.nodes.wan.only", "true")
    // specify a resource index name
    esWriteConf.set("es.resource", "testindex")
    // is the input JSON?
    esWriteConf.set("es.input.json", "yes")
    // is there a field in the mapping that should be
    // used to specify the ES document ID
    esWriteConf.set("es.mapping.id", "doc_id")
    // Disable speculation for reducer to prevent data corruption
    esWriteConf.set("mapred.reduce.tasks.speculative.execution", "false")

    /**
     * PREPPING THE DATA for Writing to ES
     *
     * Now, we need to ensure that our RDD has
     * records of the type:
     *
     *   (0, "{'some_key': 'some_value', 'doc_id': 123}")
     *
     * Note that we have an RDD of tuples. The first value
     * in the tuple (the key) actually doesn’t matter.
     * elasticsearch-hadoop only cares about the value. You
     * need to ensure that the value is a valid JSON string.
     * So, you could try starting with data like:
     *
     */

    val data = List(
      """{"key1": "some_value1", "doc_id": 100}""",
      """{"key2": "some_value2", "doc_id": 200}""",
      """{"key3": "some_value3", "doc_id": 300}""",
      """{"key4": "some_value4", "doc_id": 400}"""
    )
    // Now we can create an RDD from this:
    val rdd = spark.sparkContext.parallelize(data)
    rdd.map(JsonUtils.toJsonObject).collect().foreach(println)

    // format into an elasticsearch-hadoop compatible RDD:
    val rddFormatted = rdd.map(formatData)
    println("rddFormatted.count() : " +  rddFormatted.count())
    println("rddFormatted.collect() \n: " +  rddFormatted.collect().mkString("Array(", ", ", ")"))

    rddFormatted.saveAsNewAPIHadoopFile(
      path = "-",
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[org.elasticsearch.hadoop.mr.LinkedMapWritable],
      classOf[org.elasticsearch.hadoop.mr.EsOutputFormat],
      esWriteConf
    )

    // done!
    spark.stop()
  }
}

// object to convert a string to json object
object JsonUtils extends Serializable {
  val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  def toJsonObject(jsonString: String) :JsonNode  = {
    mapper.readTree(jsonString)
  }
 }
