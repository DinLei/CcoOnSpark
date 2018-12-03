#!/bin/bash

date=$1
diff=$2

etlClass="CCOAlgorithm"
etlJarName="/home/dinglei/project/CcoOnSpark/target/scala-2.11/CcoOnSpark-assembly-0.1.jar"

logFile="./logs/cco_"${date}".log"

sparkConf="--driver-memory 4G --num-executors 11 --executor-memory 2G --executor-cores 2 --master yarn --queue ai"

spark-submit --class ${etlClass} ${sparkConf} ${etlJarName} ${date} ${diff} > ${logFile} 2>&1


# bash run.sh 2018-09-03 2018-09-10 user
