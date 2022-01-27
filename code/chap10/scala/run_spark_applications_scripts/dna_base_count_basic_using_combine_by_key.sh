#!/bin/bash
#Download the file to data directory
INPUT_PATH="data/rs_chY.fas.gz"
if [ !  -f $INPUT_PATH ]; then
  curl -o $INPUT_PATH https://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/rs_fasta/rs_chY.fas.gz -P data
fi

if test $INPUT_PATH ; then
  ./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.DNABaseCountBasicUsingCombineByKey "--args=$INPUT_PATH"
fi