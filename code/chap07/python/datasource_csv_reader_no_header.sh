#-----------------------------------------------------
# This is a shell script to run datasource_csv_reader_no_header.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export INPUT_FILE="/book/code/chap07/sample_no_header.csv"
export SPARK_PROG="/book/code/chap07/datasource_csv_reader_no_header.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
