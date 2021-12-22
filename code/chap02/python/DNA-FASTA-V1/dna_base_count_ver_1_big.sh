#!/bin/bash
SECONDS=0
/bin/date
# do some work
#
# define Spark's installed directory
export SPARK_HOME="/book/spark-3.0.0"
#
# NOTE: define your input path
# Before running your PySpark program,
# Download chr1.subst.fa from this location:
# http://hgdownload.cse.ucsc.edu/goldenpath/hg19/snp137Mask/
# http://hgdownload.cse.ucsc.edu/goldenpath/hg19/snp137Mask/chr1.subst.fa.gz
#
INPUT_PATH="file:///book/code/chap02/chr1.subst.fa"
#
# define your PySpark program
PROG=/book/code/chap02/DNA-FASTA-V1/dna_base_count_ver_1.py
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
#
duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
