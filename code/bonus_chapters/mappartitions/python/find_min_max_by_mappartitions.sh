# set SPARK_HOME
SPARK_HOME="/Users/mparsian/spark-3.2.0"

# define your input path
INPUT_PATH="/book/code/bonus_chapters/mappartitions/SAMPLE_INPUT_FILES/"

# define your PySpark program
PROG="/book/code/bonus_chapters/mappartitions/python/find_min_max_by_mappartitions.py"

# run your program
$SPARK_HOME/bin/spark-submit  ${PROG}  ${INPUT_PATH}

