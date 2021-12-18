#-----------------------------------------------------
# This is a shell script to run dataframe_join_left.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap01/dataframe_join_left.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
