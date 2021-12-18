#-----------------------------------------------------
# This is a shell script to run average_by_key_use_foldbykey.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap01/average_by_key_use_foldbykey.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
