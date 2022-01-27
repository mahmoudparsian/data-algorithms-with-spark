#!/bin/bash
INPUT_PATH="data/sample_numbers.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.MinMaxUseMappartitions "--args=$INPUT_PATH"