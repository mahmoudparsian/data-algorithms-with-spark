#!/bin/bash
SECONDS=0
/bin/date
# do some work
#
# define your input path
INPUT_PATH="data/*.fa"

./gradlew clean run -PmainClass=org.data.algorithms.spark.ch02.DNABaseCountVER3 "--args=$INPUT_PATH"

duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
