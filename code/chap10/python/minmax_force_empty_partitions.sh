#-----------------------------------------------------
# This is a shell script to run minmax_use_mappartitions.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export INPUT_PATH="/book/code/chap10/sample_numbers.txt"
export SPARK_PROG="/book/code/chap10/minmax_force_empty_partitions.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_PATH
