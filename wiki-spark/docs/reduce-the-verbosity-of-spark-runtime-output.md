# Reduce the Verbosity of Spark Runtime Output



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


After your changes, when you re-open the shell, you should see 
less verbosity/output.
