#!/bin/bash
# define your input path
INPUT_PATH="data/sp1.fastq"

./gradlew clean run -PmainClass=org.data.algorithms.spark.ch02.DNABaseCountFastq "--args=$INPUT_PATH"