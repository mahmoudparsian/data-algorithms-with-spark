#!/bin/bash
#Download the file to data directory
INPUT_PATH="data/rs_chY.fas.gz"
if [ !  -f $INPUT_PATH ]; then
  curl -o $INPUT_PATH https://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/rs_fasta/rs_chY.fas.gz -P data
fi
#If file exists run the spark application
if test $INPUT_PATH ; then
  start_time=$(date +%s)
  ./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.DNABaseCountBasicInMapperCombinerUsingCombineByKey "--args=$INPUT_PATH"
  end_time=$(date +%s)
  # elapsed time with second resolution
  elapsed=$(( end_time - start_time ))
  echo "elapsed time (in seconds):  $elapsed"
fi