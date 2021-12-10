#-----------------------------------------------------
# This is a shell script to run top_N_use_mappartitions.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap10/top_N_use_mappartitions.py"
#
# run the PySpark program:
# find Top-3
export N = 3
$SPARK_HOME/bin/spark-submit $SPARK_PROG $N
