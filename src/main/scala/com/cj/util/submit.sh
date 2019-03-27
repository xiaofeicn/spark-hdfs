#!/usr/bin/env bash
zip -d cj-spark-mq.jar  META-INF/*.SF
zip -d cj-spark-mq.jar  META-INF/*.DSA
spark-submit --class com.cj.spark.streaming.streaming.StartStreaming  --master local[2]  --conf spark.default.parallelism=6 --executor-memory 3G  --driver-java-options "-Dlog4j.configuration=file:'log4j.properties' -Dapp_name=SparkMQ"  cj-spark-mq.jar $1
