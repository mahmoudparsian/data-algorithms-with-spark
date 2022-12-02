# Lambda Functions basics


     @author: Mahmoud Parsian
              Ph.D. in Computer Science
              email: mahmoud.parsian@yahoo.com
              
	Last updated: December 1, 2022

----------

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

-----------

	Lambda functions = Anonymous functions
	
A lambda function is a small function containing a single expression. 
Lambda functions can also act as anonymous functions where they don’t 
require any name. These are very helpful when we have to perform small 
tasks with less code.


# Python Example

Python programming language supports the creation of anonymous functions 
(i.e. functions defined without a name), using a construct called `lambda`.

The general structure of a lambda function is:

 	lambda <arguments>: <expression>
 

Here is a simple python function to double the value of a scalar:

	 def f(x): 
	   return x*2
     #end-def

For instance to use this function:

	 print(f(4))
	 8
 

The same function can be written as `lambda` function:

 	>>> g = lambda x: x*2
	>>> g
	<function <lambda> at 0x105668b90>
	>>> g(10)
	20
	

# PySpark Example

You may use lambda expressions or functions in PySpark:


### PySpark Example using `lambda` 

	# rdd: RDD[(String, Integer)]
	# x : (String, Integer)
	rdd2 = rdd.filter(lambda x: x[1] > 0)
	

### PySpark Example using function

	# x : (String, Integer)
	def filter_function(x):
		if (x[1] > 0):
			return True
		else:
			return False
	#end-def

	# rdd: RDD[(String, Integer)]
	rdd2 = rdd.filter(filter_function)

