#!/bin/bash
REDIS_SERVER="localhost"
REDIS_PORT="6379"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceRedisReader "--args=$REDIS_SERVER $REDIS_PORT"
