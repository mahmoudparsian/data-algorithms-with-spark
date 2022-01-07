#!/bin/bash
SECONDS=0
/bin/date
# do some work
#
# NOTE: define your input path
# Before running your Spark program,
# Download chr1.subst.fa from this location:
# http://hgdownload.cse.ucsc.edu/goldenpath/hg19/snp137Mask/
# http://hgdownload.cse.ucsc.edu/goldenpath/hg19/snp137Mask/chr1.subst.fa.gz
#
INPUT_PATH="data/chr1.subst.fa"

./gradlew clean run -PmainClass=org.data.algorithms.spark.ch02.DNABaseCountVER3 "--args=$INPUT_PATH"

duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
