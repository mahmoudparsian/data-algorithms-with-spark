start_time=$(date +%s)
#
INPUT_PATH=~/Downloads/rs_chY.fas
$SPARK_HOME/bin/spark-submit dna_base_count_inmapper_combiner_using_combinebykey.py $INPUT_PATH
#
end_time=$(date +%s)
# elapsed time with second resolution
elapsed=$(( end_time - start_time ))
echo "elapsed time (in seconds):  $elapsed"
