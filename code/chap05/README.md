# Partitions and Executors in MapReduce and Beyond

	Author: Mahmoud Parsian
	Last updated: 9/28/2022
	
## Introduction to MapReduce and Beyond 

MapReduce is a parallel programming model 
and an associated implementation introduced 
by Google. In the programming model, a user 
specifies the computation by two functions, 
`map()` and `reduce()`. MapReduce paradigm
is implemented by many systems:

* Google MapReduce (proprietary, not open sourced)
* Apache Hadoop (open source)
* Apache Tez (open source)
* Apache Spark (open source)
	* Spark is a superset of MapReduce and does 
	  every analytics much better than Hadoop.
	  Spark supports superset of `map()` and 
	  `reduce()` functions
	* Spark utilizes RAM/memory but Hadoop is
	  mostly disk based.
	* Spark is a multi-language engine for executing 
	  data engineering, data science, and machine 
	  learning on single-node machines or clusters.
	* Spark is preferred over Hadoop (Spark can be 
	  10x to 100x times faster than Hadoop)
	 
	
## Parallelism in MapReduce-based Systems

The MapReduce programming model is created 
for processing data which requires "DATA 
PARALLELISM", the ability to compute multiple 
independent operations in any order. After the 
MapReduce system receives a job, it first divides 
all the input data of the job into several data 
blocks of equal size (partitions data into smaller
chunks called partitions). For example, each `map()` 
task is responsible for  processing a data block (a 
partition). All `map()` tasks (mappers) are executed 
at the same time independently, forming parallel 
processing of data. Also, all `reduce()` tasks 
(reducers) are executed at the same time independently, 
forming parallel processing of reducers. Therefore,
MapReduce allows parallel and distributed processing. 
Each feature selector can be divided into subtasks and 
the subtasks can then be processed in parallel. 

Note that Apache Spark is a superset of MapReduce 
and beyond. When a task is parallelized in Spark, 
it means that concurrent tasks may be running on the 
driver node or worker nodes. How the task is split 
across these different nodes in the cluster depends 
on the types of data structures and libraries that 
you're using.

The concept of data partitioning semi-equally applies 
to MapReduce and Spark systems.


## Input:

Your input is partitioned into chunks called partitions.
For example, if you have `80,000,000,000` records (data points)
and you partition it into `40,000` chunks: then

* The number of partitions: `40,000`
* Approximate number of elements per partition: `2,000,000`
* `40,000 x 2,000,000 = 80,000,000,000`
* Let's label partitions as `P_1`, `P_2`, ..., `P_40000`


## Cluster:

Now assume that you have a cluster of 4 nodes: 
one master (denoted by M), and 3 working nodes 
(W1, W2, W3). Therefore our example cluster is
denoted as: `C = {M, W1, W2, W3}`. Also, note that 
here we assume that the master node acts as a 
cluster manager and does not execute any 
transformation (such as mappers, filters, reducers, 
...).  Basically, we assume that master node is a 
cluster manager: manage the cluster activities and 
functionalities.  Further assume that each worker 
node can have 4 executors (therefore the total number
of available executors will be 12 = 3 x 4). The number 
of executors depends on the size and power of a worker 
node: if you have realy a powerful (lots of RAM and 
CPU) worker node, then it even be able to handle 10 
to 16 executors. Therefore, the number of executors 
in our cluster is `12 (3 x 4)`. We denote our assumed 
cluster as `C = {M, W1, W2, W3}`.

## Distributing Partitions to Worker Nodes

The question is that how the cluster manager 
will distribute and execute 40,000 partitions 
among 3 worker nodes (in our example, we have 
4 executors per worker node -- each executor
can execute a mapper, filter, or reducer). Let's 
assume that these `40,000` partitions are queued 
to be processed by the cluster manager.  Let's 
label our executors as: 

      E: {E_1, E_2, E_3, ..., E_12} 
      
Lets's say that the first transformation is a 
mapper (i.e., a `map()` function) to be executed 
by a `map()` function (basically a `map()` function 
receives a single partition and then emits a set 
of (key, value) pairs). The first iteration: 12 
partitions are assigned to E (i.e., the 12 executors, 
each executor gets a single partition and executes 
`map()` function on that given partition). When an 
executor finishes the execution of map(single_partition), 
then it sends the result to the cluster manager and 
then cluster manager assigns another partition from 
a queue. This process continues until we exhaust all 
partitions. Therefore at any one time, 12 executors 
are working (executing the `map()` function) in parallel 
and independently. The more worker nodes we have, the 
faster we will execute the whole thing. 

Given our initial cluster as C, for example if it 
takes T seconds to complete the execution of 40,000 
partitions with 3 worker nodes `{W1, W2, W3}`, then 

* By adding an additional 3 worker nodes `{W4, W5, W6}` 
will reduce the execution time by about `T/2`. 

* If we increase the number of worker nodes to 9 (one 
master node and 9 worker nodes), then the elapsed time 
will be reduced to about `T/3`.

 

