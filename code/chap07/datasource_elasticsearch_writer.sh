#-----------------------------------------------------------
# This is a shell script to run datasource_elasticsearch_writer.py
#-----------------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap07/datasource_elasticsearch_writer.py"
export ELASTIC_SEARCH_HOST="localhost"
export JAR="/book/code/jars/elasticsearch-hadoop-6.4.2.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars "${JAR}" $SPARK_PROG ${ELASTIC_SEARCH_HOST}
