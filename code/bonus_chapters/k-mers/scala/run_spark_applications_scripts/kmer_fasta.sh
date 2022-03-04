#!/bin/bash
INPUT_PATH="data/sample_1.fasta"
K=4
N=3
./gradlew clean run -PmainClass=org.data.algorithms.spark.bonuschapter.KMERFasta "--args=$INPUT_PATH $K $N"
