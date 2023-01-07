# Word Count

The purpose of this folder is to present 
multiple solutions (using DataFrames and RDDs)
for classic word count problem.

------

<table>
<tr>
<td>
<a href="https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/">
<img src="https://learning.oreilly.com/library/cover/9781492082378/250w/">
</a>
</td>
<td>
"... This  book  will be a  great resource for <br>
both readers looking  to  implement  existing <br>
algorithms in a scalable fashion and readers <br>
who are developing new, custom algorithms  <br>
using Spark. ..." <br>
<br>
<a href="https://cs.stanford.edu/people/matei/">Dr. Matei Zaharia</a><br>
Original Creator of Apache Spark <br>
<br>
<a href="https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/docs/FOREWORD_by_Dr_Matei_Zaharia.md">FOREWORD by Dr. Matei Zaharia</a><br>
</td>
</tr>   
</table>

--------

## Word Count using Spark DataFrames

	word_count_by_dataframe.log
	word_count_by_dataframe.py
	
	word_count_by_dataframe_shorthand.log
	word_count_by_dataframe_shorthand.py

------

## Word Count using Spark RDDs

Solutions are provided by using `reduceByKey()`
, `groupByKey()`, and `combineByKey()` reducers. 
In general, solution by using `reduceByKey()` and
`combineByKey()` are  scale-out solutions
than using `groupByKey()`.


	wordcount_by_groupbykey.py
	wordcount_by_groupbykey.sh
	
	wordcount_by_groupbykey_shorthand.py
	wordcount_by_groupbykey_shorthand.sh
	
	wordcount_by_combinebykey.py
	wordcount_by_combinebykey.sh
	
	wordcount_by_reducebykey.py
	wordcount_by_reducebykey.sh
	
	wordcount_by_reducebykey_shorthand.py
	wordcount_by_reducebykey_shorthand.sh
	
	wordcount_by_reducebykey_with_filter.py
	wordcount_by_reducebykey_with_filter.sh


------

best regards, <br>
Mahmoud Parsian
