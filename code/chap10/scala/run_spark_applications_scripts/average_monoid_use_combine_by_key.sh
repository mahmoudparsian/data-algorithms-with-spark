#!/bin/bash
INPUT="data/sample_input.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.AverageMonoidUseCombineByKey "--args= $INPUT"
