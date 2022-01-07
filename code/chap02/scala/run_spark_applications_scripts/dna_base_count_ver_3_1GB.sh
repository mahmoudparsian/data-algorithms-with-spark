#!/bin/bash
SECONDS=0
/bin/date
#------------------------------------------------------
# NOTE: define your input path
# Before running your Spark program,
# Download *.fa from this location and place it under
# the following directory: /book/chap02/data/
#
# Download URL:
#   http://hgdownload.cse.ucsc.edu/goldenpath/hg19/snp137Mask/
#------------------------------------------------------
INPUT_PATH="data/*.fa"

./gradlew clean run -PmainClass=org.data.algorithms.spark.ch02.DNABaseCountVER3 "--args=$INPUT_PATH"

duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
