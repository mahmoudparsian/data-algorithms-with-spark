# define PySpark program
export PROG="/book/code/chap10/average_monoid_use_aggregatebykey.py"
# define your input path
export INPUT="/book/code/chap10/sample_input.txt"
# define your Spark home directory
export SPARK_HOME="/book/spark-3.2.0"
# run the program
$SPARK_HOME/bin/spark-submit $PROG $INPUT
