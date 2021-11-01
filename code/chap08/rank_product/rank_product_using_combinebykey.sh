export SPARK_HOME="/home/book/spark-3.2.0"
export SPARK_PROG="/home/book/code/chap08/rank_product/rank_product_using_combinebykey.py"
#
export K=3
#
export STUDY_1="/home/book/code/chap08/rank_product/sample_input/rp1.txt"
export STUDY_2="/home/book/code/chap08/rank_product/sample_input/rp2.txt"
export STUDY_3="/home/book/code/chap08/rank_product/sample_input/rp3.txt"
#
export OUTPUT="/tmp/rank_product/output"
#
${SPARK_HOME}/bin/spark-submit ${SPARK_PROG} ${OUTPUT} ${K} ${STUDY_1} ${STUDY_2} ${STUDY_3}

output_path /tmp/rankproduct/ouput
K 3
studies_input_path [
'/home/book/code/chap08/rank_product/sample_input/rp1.txt', 
'/home/book/code/chap08/rank_product/sample_input/rp2.txt', 
'/home/book/code/chap08/rank_product/sample_input/rp3.txt'
]

input_path /home/book/code/chap08/rank_product/sample_input/rp1.txt
raw_genes ['K_1,30.0', 'K_2,60.0', 'K_3,10.0', 'K_4,80.0']
genes [('K_1', 30.0), ('K_2', 60.0), ('K_3', 10.0), ('K_4', 80.0)]
genes_combined [('K_2', (60.0, 1)), ('K_3', (10.0, 1)), ('K_1', (30.0, 1)), ('K_4', (80.0, 1))]
input_path /home/book/code/chap08/rank_product/sample_input/rp2.txt
raw_genes ['K_1,90.0', 'K_2,70.0', 'K_3,40.0', 'K_4,50.0']
genes [('K_1', 90.0), ('K_2', 70.0), ('K_3', 40.0), ('K_4', 50.0)]
genes_combined [('K_2', (70.0, 1)), ('K_3', (40.0, 1)), ('K_1', (90.0, 1)), ('K_4', (50.0, 1))]
input_path /home/book/code/chap08/rank_product/sample_input/rp3.txt
raw_genes ['K_1,4.0', 'K_2,8.0']
genes [('K_1', 4.0), ('K_2', 8.0)]
genes_combined [('K_2', (8.0, 1)), ('K_1', (4.0, 1))]
sorted_rdd [(80.0, 'K_4'), (60.0, 'K_2'), (30.0, 'K_1'), (10.0, 'K_3')]
indexed [((80.0, 'K_4'), 0), ((60.0, 'K_2'), 1), ((30.0, 'K_1'), 2), ((10.0, 'K_3'), 3)]
ranked [('K_4', 1), ('K_2', 2), ('K_1', 3), ('K_3', 4)]
sorted_rdd [(90.0, 'K_1'), (70.0, 'K_2'), (50.0, 'K_4'), (40.0, 'K_3')]
indexed [((90.0, 'K_1'), 0), ((70.0, 'K_2'), 1), ((50.0, 'K_4'), 2), ((40.0, 'K_3'), 3)]
ranked [('K_1', 1), ('K_2', 2), ('K_4', 3), ('K_3', 4)]
sorted_rdd [(8.0, 'K_2'), (4.0, 'K_1')]
indexed [((8.0, 'K_2'), 0), ((4.0, 'K_1'), 1)]
ranked [('K_2', 1), ('K_1', 2)]
