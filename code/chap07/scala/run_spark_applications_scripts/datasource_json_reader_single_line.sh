#!/bin/bash
INPUT_PATH="data/sample_single_line.json"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceJSONReaderSingleLine "--args=$INPUT_PATH"
