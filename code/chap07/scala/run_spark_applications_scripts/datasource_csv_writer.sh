#!/bin/bash
export OUTPUT_CSV_FILE_PATH="data/tmp/output"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceCSVWriter "--args=$OUTPUT_CSV_FILE_PATH"
