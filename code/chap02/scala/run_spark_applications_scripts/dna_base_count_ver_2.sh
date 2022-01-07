#!/bin/bash
# define your input path
INPUT_PATH="data/sample.fasta"

./gradlew clean run -PmainClass=org.data.algorithms.spark.ch02.DNABaseCountVER2 "--args=$INPUT_PATH"

