#-----------------------------------------------------
# This is a shell script for finding averages per
# key by using the aggregateByKey() transformation  
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap04/average_by_key_use_aggregatebykey.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG 

