#!/bin/bash
INPUT_PATH="data/urls.txt"
NUMBER_OF_ITERATIONS=5
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch08.PageRank "--args=$INPUT_PATH $NUMBER_OF_ITERATIONS"
