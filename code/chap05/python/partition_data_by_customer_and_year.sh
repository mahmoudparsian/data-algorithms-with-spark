#-----------------------------------------------------
# This is a shell script to run the following program:
#      partition_data_by_customer_and_year.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export INPUT_PATH="/book/code/chap05/customers.txt"
export OUTPUT_PATH="/tmp/partition_demo"
export SPARK_PROG="/book/code/chap05/partition_data_by_customer_and_year.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_PATH $OUTPUT_PATH
