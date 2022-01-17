#!/bin/bash
MONGODB_URI="mongodb://localhost:27017/test.coll66"
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch07.DatasourceMongodbReader "--args=$MONGODB_URI"
