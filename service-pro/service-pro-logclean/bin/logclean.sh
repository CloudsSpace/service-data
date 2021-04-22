#!/bin/sh

function application(){

/usr/local/software/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master local --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --conf spark.default.parallelism=${parallelism} --class com.controller.${mainClass} --jars /root/jars/${jar}-jar-with-dependencies.jar
}
driverMemory=1g
executorMemory=1g
numberExecutors=1
executorCores=2
parallelism=150
mainClass=LogDetail
jar=LogDetail
#src=hdfs://datanode:9000/data/ods/cat-end-logconsume.log.2019-09-05
#dst=hdfs://datanode:9000/data/dwo/logClean
application