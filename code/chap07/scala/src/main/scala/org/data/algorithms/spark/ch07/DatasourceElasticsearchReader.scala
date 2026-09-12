package org.data.algorithms.spark.ch07

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.mr.LinkedMapWritable

/**
 *-----------------------------------------------------
 * Read From ElasticSearch and write to an RDD
 * Input: ElasticSearch Hostname
 *------------------------------------------------------
 * Input Parameters:
 *    ElasticSearch Hostname
 *-------------------------------------------------------
 *
 * @author Biman Mandal
 *-------------------------------------------------------
 */
object DatasourceElasticsearchReader {

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      System.err.println("Pass Elasticsearch Server and Port in Command Line Argument")
      System.exit(-1)
    }

    // read name of ElasticSearch Hostname
    val esHostname = args(0)
    println("esHostname : " + esHostname)

    val esPort = args(1)
    println("esPort : " + esPort)

    // create an instance of SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.classesToRegister", "org.apache.hadoop.io.Text")
      .getOrCreate()

    /**
     * Read from ElasticSearch and Write to RDD
     *
     * Now that we have some data in Elasticsearch
     * (by running DatasourceElasticsearchWriter),
     * we can read it back in using the elasticsearc-hadoop
     * connector.
     *
     */
    val esReadConf = new Configuration()
    // specify the node that we are sending data to
    // (this should be the master node)
    esReadConf.set("es.nodes", esHostname)
    // specify the port in case it is not the default port
    esReadConf.set("es.port", esPort)
    // Comment it out in case your elastic search is not running in local docker
    esReadConf.set("es.nodes.wan.only", "true")
    // specify a resource index name
    esReadConf.set("es.resource", "testindex")

    // Now we can generate an RDD from this Elasticsearch data:
    val esRdd = spark.sparkContext.newAPIHadoopRDD(
      esReadConf,
      classOf[org.elasticsearch.hadoop.mr.EsInputFormat[NullWritable,LinkedMapWritable]],
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[org.elasticsearch.hadoop.mr.LinkedMapWritable]
    )
    println("esRdd : " +  esRdd)
    println("esRdd.count() : " +  esRdd.count())
    println("esRdd.collect() \n: " + esRdd.collect().mkString("Array(", ", ", ")"))

    // done!
    spark.stop()


  }
}
