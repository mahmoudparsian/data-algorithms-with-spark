# define Spark's installed directory
export SPARK_HOME="/book/spark-3.2.0"
#
# define your input path
INPUT_PATH="$SPARK_HOME/NOTICE"
#
# define your PySpark program
PROG="/book/code/wordcount/wordcount_by_reducebykey_with_filter.py"

# drop words if its length are less than 3
WORD_LENGTH_THRESHOLD=3

# drop words (after reduction) if its frequency is less than 2
FREQUENCY_THRESHOLD=2

# submit your spark application
$SPARK_HOME/bin/spark-submit ${PROG} ${INPUT_PATH} ${WORD_LENGTH_THRESHOLD} ${FREQUENCY_THRESHOLD}
