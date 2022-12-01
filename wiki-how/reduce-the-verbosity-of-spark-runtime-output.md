When using PySpark, you may find the logging statements 
that get printed in the shell are distracting (it prints
out lots of debugging information).

Let `SPARK_HOME` be an environment variable, which points
to your Spark installation directory: for example:

		export SPARK_HOME="/home/mparsian/spark-3.3.1"
		
You can control the verbosity of the logging. To do this, 
you can create a new file in the `$SPARK_HOME/conf/` directory 
called `log4j2.properties`. The Spark distribution already 
include a template for this file called `log4j2.properties.template`. 
To make the logging less verbose, make a copy of 
`$SPARK_HOME/conf/log4j.properties.template` called 
`$SPARK_HOME/conf/log4j2.properties` and find the following line:

		rootLogger.level = info

Then lower (change) the log level so that we only show `WARN` 
(warning) message and above by changing it to the following:

		rootLogger.level = warn


After your changes, when you re-open the shell, you should see less output.
