#!/bin/bash
OUTPUT_PATH="data/tmp/text-file-out"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceTextfileWriter "--args=$OUTPUT_PATH"
