#!/bin/bash
INPUT_PATH="data/sample_document.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.bonuschapter.WordCountByReduceByKeyShorthand "--args=$INPUT_PATH"
