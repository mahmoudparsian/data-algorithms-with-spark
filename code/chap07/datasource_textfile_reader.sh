#-----------------------------------------------------
# This is a shell script to run datasource_textfile_reader.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export INPUT_FILE="/book/code/chap07/sample_numbers.txt"
export SPARK_PROG="/book/code/chap07/datasource_textfile_reader.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
