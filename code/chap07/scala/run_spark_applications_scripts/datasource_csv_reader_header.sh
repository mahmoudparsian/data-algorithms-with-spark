#!/bin/bash
INPUT_PATH="data/sample_with_header.csv"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceCSVReaderHeader "--args=$INPUT_PATH"