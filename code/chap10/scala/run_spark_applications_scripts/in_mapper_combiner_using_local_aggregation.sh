#!/bin/bash
INPUT_PATH="data/sample_dna_seq.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.InMapperCombinerUsingLocalAggregation "--args=$INPUT_PATH"
