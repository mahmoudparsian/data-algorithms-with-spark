# define Spark's installed directory
export SPARK_HOME="/book/spark-3.2.0"
#
# define your input path
INPUT_PATH="$SPARK_HOME/NOTICE"
#
# define your PySpark program
PROG="/book/code/wordcount/wordcount_by_reducebykey.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit ${PROG} ${INPUT_PATH} 
