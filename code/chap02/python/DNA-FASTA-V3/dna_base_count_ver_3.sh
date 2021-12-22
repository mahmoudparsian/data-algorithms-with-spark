# define Spark's installed directory
export SPARK_HOME="/book/spark-3.0.0"
#
# define your input path
INPUT_PATH="file:///book/code/chap02/data/sample.fasta"
#
# define your PySpark program
PROG="/book/code/chap02/DNA-FASTA-V3/dna_base_count_ver_3.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
