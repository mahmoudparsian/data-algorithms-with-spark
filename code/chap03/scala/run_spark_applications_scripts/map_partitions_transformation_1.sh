#!/bin/bash
INPUT_PATH="data/sample_numbers.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch03.MapPartitionsTransformation1 "--args=$INPUT_PATH"
