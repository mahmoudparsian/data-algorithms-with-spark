# What are [Lambda Functions](./Lambda_Expressions.pdf)?

A lambda function is a small function containing a single expression. 
Lambda functions can also act as anonymous functions where they don’t 
require any name. These are very helpful when we have to perform small 
tasks with less code.

````

# rdd: RDD[(String, Integer)]
# x : (String, Integer)
rdd2 = rdd.filter(lambda x: x[1] > 0)

-- OR --

# x : (String, Integer)
def filter_function(x):
    if (x[1] > 0):
        return True
    else:
        return False
#end-def

# rdd: RDD[(String, Integer)]
rdd2 = rdd.filter(filter_function)

````
