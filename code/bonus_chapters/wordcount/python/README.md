# Word Count

The purpose of this folder is to present 
multiple solutions for classic word count 
problem.

## Word Count using Spark DataFrames

		word_count_by_dataframe.log
		word_count_by_dataframe.py
		word_count_by_dataframe_shorthand.log
		word_count_by_dataframe_shorthand.py

## Word Count using Spark RDDs

Solutions are provided by using reduceByKey()
and groupByKey() reducers. In general, solution
by using reduceByKey() is a scale-out solution
than using groupByKey().

		wordcount_by_groupbykey.py
		wordcount_by_groupbykey.sh
		wordcount_by_groupbykey_shorthand.py
		wordcount_by_groupbykey_shorthand.sh
		wordcount_by_reducebykey.py
		wordcount_by_reducebykey.sh
		wordcount_by_reducebykey_shorthand.py
		wordcount_by_reducebykey_shorthand.sh
		wordcount_by_reducebykey_with_filter.py
		wordcount_by_reducebykey_with_filter.sh


	best regards,
	Mahmoud Parsian
