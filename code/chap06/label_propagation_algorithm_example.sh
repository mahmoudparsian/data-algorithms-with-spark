#-----------------------------------------------------
# This is a shell script for 
#   1. building a graph using GraphFrames package.
#   2. applying Label Propagation Algorithm (LPA)
#
# Reference: https://en.wikipedia.org/wiki/Label_Propagation_Algorithm
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/home/book/spark-3.2.0"
export SPARK_PROG="/home/book/code/chap06/label_propagation_algorithm_example.py"
export GRAPH_FRAMES="graphframes:graphframes:0.8.2-spark3.2-s_2.12"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --packages $GRAPH_FRAMES $SPARK_PROG 
