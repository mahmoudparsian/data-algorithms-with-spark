# define PySpark program
export PROG="/book/code/chap10/inmapper_combiner_local_aggregation.py"
# define your input path
export INPUT="/book/code/chap10/sample_dna_seq.txt"
# define your Spark home directory
export SPARK_HOME="/book/spark-3.2.0"
# run the program
$SPARK_HOME/bin/spark-submit $PROG $INPUT
