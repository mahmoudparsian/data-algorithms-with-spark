#!/bin/bash
SECONDS=0
/bin/date
# do some work
#
# define Spark's installed directory
export SPARK_HOME="/book/spark-3.0.0"
#
# define your input path
INPUT_PATH="file:///book/code/chap02/data/*.fasta"
#
# define your PySpark program
PROG="/book/code/chap02/DNA-FASTA-V1/dna_base_count_ver_1.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
#
duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
