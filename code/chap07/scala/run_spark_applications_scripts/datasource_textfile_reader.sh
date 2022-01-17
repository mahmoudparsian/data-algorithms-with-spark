#!/bin/bash
INPUT_PATH="data/sample_numbers.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceTextfileReader "--args=$INPUT_PATH"
