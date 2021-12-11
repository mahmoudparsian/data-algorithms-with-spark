#-----------------------------------------------------
# This is a shell script to run datasource_textfile_writer.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export OUTPUT_PATH="/tmp/zoutput"
export SPARK_PROG="/book/code/chap07/datasource_textfile_writer.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $OUTPUT_PATH
