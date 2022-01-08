#!/bin/bash
INPUT_PATH="data/bigrams_input.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch03.FlatMapTransformation1FromFile "--args=$INPUT_PATH"
