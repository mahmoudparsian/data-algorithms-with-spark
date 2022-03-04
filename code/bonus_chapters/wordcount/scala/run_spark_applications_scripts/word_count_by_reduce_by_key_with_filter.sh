#!/bin/bash
INPUT_PATH="data/sample_document.txt"
# drop words if its length are less than 3
WORD_LENGTH_THRESHOLD=3
# drop words (after reduction) if its frequency is less than 2
FREQUENCY_THRESHOLD=2
./gradlew clean run -PmainClass=org.data.algorithms.spark.bonuschapter.WordCountByReduceByKeyWithFilter "--args=$INPUT_PATH $WORD_LENGTH_THRESHOLD $FREQUENCY_THRESHOLD"