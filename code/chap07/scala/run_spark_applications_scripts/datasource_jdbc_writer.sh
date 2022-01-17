#!/bin/bash
JDBC_URL=jdbc:mysql://localhost/metadb
JDBC_DRIVER=com.mysql.cj.jdbc.Driver
JDBC_TARGET_TABLE_NAME=people
JDBC_USER=root
JDBC_PASSWORD=my-secret-pw
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceJDBCWriter "--args=$JDBC_URL $JDBC_DRIVER $JDBC_USER $JDBC_PASSWORD $JDBC_TARGET_TABLE_NAME"
