#-----------------------------------------------------
# This is a shell script for building  
#   1. a graph using GraphFrames package.
#   2. find unique Triangles from the built graph
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/home/book/spark-3.2.0"
export SPARK_PROG="/home/book/code/chap06/unique_triangles_finder.py"
export GRAPH_FRAMES="graphframes:graphframes:0.8.2-spark3.2-s_2.12"
#
export VERTICES_PATH="/home/book/code/chap06/sample_graph_vertices.txt"
export EDGES_PATH="/home/book/code/chap06/sample_graph_edges.txt"
#
# run the PySpark program:
${SPARK_HOME}/bin/spark-submit --packages ${GRAPH_FRAMES} ${SPARK_PROG} ${VERTICES_PATH} ${EDGES_PATH}
