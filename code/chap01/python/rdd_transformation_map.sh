#-----------------------------------------------------
# This is a shell script to run rdd_transformation_map.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap01/rdd_transformation_map.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
