#!/bin/bash
INPUT_PATH="data/sample_input.csv"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch03.MapTransformation1FromFile "--args=$INPUT_PATH"
