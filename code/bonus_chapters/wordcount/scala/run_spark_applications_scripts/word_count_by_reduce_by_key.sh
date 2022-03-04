#!/bin/bash
INPUT_PATH="data/sample_document.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.bonuschapter.WordCountByReduceByKey "--args=$INPUT_PATH"
