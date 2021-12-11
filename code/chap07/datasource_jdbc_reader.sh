#-----------------------------------------------------
# This is a shell script to run datasource_jdbc_reader.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/book/spark-3.2.0"
export SPARK_PROG="/book/code/chap07/datasource_jdbc_reader.py"
#
# define the required MySQL database connection parameters
export JDBC_URL="jdbc:mysql://localhost/metadb"
export JDBC_DRIVER="com.mysql.jdbc.Driver"
export JDBC_USER="root"
export JDBC_PASSWORD="mp22_password"
export JDBC_SOURCE_TABLE_NAME="dept"
#
# define the required JAR file for MySQL database access
export JAR="/book/code/jars/mysql-connector-java-5.1.42.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars ${JAR}  ${SPARK_PROG} ${JDBC_URL} ${JDBC_DRIVER} ${JDBC_USER} ${JDBC_PASSWORD} ${JDBC_SOURCE_TABLE_NAME}
