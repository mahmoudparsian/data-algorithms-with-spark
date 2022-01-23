#!/bin/bash
OUTPUT_PATH="data/tmp/rank-product-group-by-key"
NUMBER_OF_STUDIES=3
INPUT_PATH_FOR_STUDY_1="data/sample_input/rp1.txt"
INPUT_PATH_FOR_STUDY_2="data/sample_input/rp2.txt"
INPUT_PATH_FOR_STUDY_3="data/sample_input/rp3.txt"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch08.RankProductUsingGroupByKey "--args=$OUTPUT_PATH $NUMBER_OF_STUDIES $INPUT_PATH_FOR_STUDY_1 $INPUT_PATH_FOR_STUDY_2 $INPUT_PATH_FOR_STUDY_3"
