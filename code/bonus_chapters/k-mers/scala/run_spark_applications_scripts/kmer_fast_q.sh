#!/bin/bash
INPUT_PATH="data/sample_1.fastq"
K=4
N=3
./gradlew clean run -PmainClass=org.data.algorithms.spark.bonuschapter.KMERFastQ "--args=$INPUT_PATH $K $N"