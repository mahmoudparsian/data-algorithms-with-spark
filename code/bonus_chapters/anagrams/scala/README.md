![Anagram](https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/images/anagram.png)

----

What is an Anagram?
===================
* Anagram is a word or phrase made by transposing the letters 
of another word or phrase.  
* An anagram is basically a different arrangement of letters in a word. 
* Anagram does not need to be meaningful.
* An anagram is a type of word play, the result of rearranging 
the letters of a word or phrase to produce a new word or phrase, 
using all the original letters exactly once
* For example, the word "anagram" can be rearranged into "nagaram". 
"Elvis" can be rearranged into "lives", therefore "Elvis" and "lives"
are anagrams
* Someone who creates anagrams may be called an "anagrammatist"
* Here we work on anagrams, which are only single words. 

----

Examples
======== 
* The word "mary" can be rearranged into "army"
* The word "secure" is an anagram of "rescue"
* The word "elvis" can be rearranged into "lives"

-----

Problem
=======
Write a Spark code to find anagrams in a text file

-------

Sample Dataset
=============
The file is located at /book/code/anagrams/sample_document.txt

-------


Programs
========
The purpose of these Spark programs are to find anagrams 
for a set of given  documents.

| Program Name                                                        | Description                               | Script                                                          | 
|---------------------------------------------------------------------|-------------------------------------------|-----------------------------------------------------------------|
| `org.data.algorithms.spark.bonuschapter.AnagramsByCombineByKey`     |  Solution using `combineByKey()` reducer  | ./run_spark_applications_scripts/anagrams_by_combine_by_key.sh  |
| `org.data.algorithms.spark.bonuschapter.AnagramsByReduceByKey`      |  Solution using `reduceByKey()` reducer   | ./run_spark_applications_scripts/anagrams_by_reduce_by_key.sh   |
| `org.data.algorithms.spark.bonuschapter.AnagramsByGroupByKey`       |  Solution using `groupByKey()` reducer    | ./run_spark_applications_scripts/anagrams_by_group_by_key.sh    |


We will ignore words if their length is less than 1.

------

References  
==========
* http://en.wikipedia.org/wiki/Anagram
* http://www.merriam-webster.com/dictionary/anagram


[![Data Algorithms with Spark](https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/images/data_algorithms_with_spark.jpg)](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) 


