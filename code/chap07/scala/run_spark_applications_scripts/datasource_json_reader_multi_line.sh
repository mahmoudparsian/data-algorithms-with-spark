#!/bin/bash
INPUT_PATH="data/sample_multi_line.json"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceJSONReaderMultiLine "--args=$INPUT_PATH"
