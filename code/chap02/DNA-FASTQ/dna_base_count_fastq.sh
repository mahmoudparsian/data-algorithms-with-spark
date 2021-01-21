# define Spark's installed directory
export SPARK_HOME="/book/spark-3.0.0"
#
# define your input path
INPUT_PATH="file:///book/code/chap02/data/sp1.fastq"
#
# define your PySpark program
PROG="/book/code/chap02/DNA-FASTQ/dna_base_count_fastq.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
