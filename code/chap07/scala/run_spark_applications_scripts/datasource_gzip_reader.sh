#!/bin/bash
INPUT_PATH="data/sample_no_header.csv.gz"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceGZIPReader "--args=$INPUT_PATH"
