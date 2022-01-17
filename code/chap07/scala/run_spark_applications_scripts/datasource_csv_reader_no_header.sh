#!/bin/bash

INPUT_PATH="data/sample_no_header.csv"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceCSVReaderNoHeader "--args=$INPUT_PATH"
