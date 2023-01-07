# define Spark's installed directory
export SPARK_HOME="/home/spark-3.3.1"
#
# define your input path
INPUT_PATH="${SPARK_HOME}/NOTICE"
#
# define your PySpark program
PROG="wordcount_by_combinebykey.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit ${PROG} ${INPUT_PATH} 
