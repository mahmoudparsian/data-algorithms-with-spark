#---------------------------------------------------------
begin=`/bin/date`
echo "begin=${begin}"
START_TIME=$(date +%s)
#command block that takes time to complete...
#---------------------------------------------------------
#
export SPARK_HOME="/Users/mparsian/spark-3.3.0"
export PROG="Top_N_movies_RDD_using_groupByKey.py"
export INPUT_PATH="/Users/mparsian/Downloads/ml-25m"
export ratings="${INPUT_PATH}/ratings.csv"
export movies="${INPUT_PATH}/movies.csv"
export rating_threshold="0"
#
# for top 10
export N=10
#
# sys.argv[x]                        1     2          3         4
$SPARK_HOME/bin/spark-submit ${PROG} ${N}  ${ratings} ${movies} ${rating_threshold}
#
#---------------------------------------------------------
end=`/bin/date`
echo "end=${end}"
END_TIME=$(date +%s)
echo "elapsed time: $((${END_TIME} - ${START_TIME})) seconds to complete this task."
