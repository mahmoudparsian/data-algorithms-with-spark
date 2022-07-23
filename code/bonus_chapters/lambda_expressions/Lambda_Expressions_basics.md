# Lambda Functions basics

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

