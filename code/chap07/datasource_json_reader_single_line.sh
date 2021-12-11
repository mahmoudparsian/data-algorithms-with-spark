#-----------------------------------------------------
# This is a shell script to run datasource_json_reader_single_line.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export INPUT_FILE="/book/code/chap07/sample_single_line.json"
export SPARK_PROG="/book/code/chap07/datasource_json_reader_single_line.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
