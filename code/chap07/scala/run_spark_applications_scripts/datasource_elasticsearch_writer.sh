#!/bin/bash
ELASTICSEARCH_SERVER="localhost"
ELASTICSEARCH_PORT="9200"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceElasticsearchWriter "--args=$ELASTICSEARCH_SERVER $ELASTICSEARCH_PORT"
