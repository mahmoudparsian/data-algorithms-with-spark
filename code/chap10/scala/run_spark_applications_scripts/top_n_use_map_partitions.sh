#!/bin/bash
N=3
./gradlew clean run -PmainClass=org.data.algorithms.spark.ch10.TopNUseMapPartitions "--args=$N"
