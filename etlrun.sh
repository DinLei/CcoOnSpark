#!/bin/bash

dateBegin=$1
dateEnd=$2

etlClass="DailyETLJob"
etlJarName="/home/dinglei/project/CcoOnSpark/target/scala-2.11/CcoOnSpark-assembly-0.1.jar"

sparkConf="--driver-memory 4G --num-executors 11 --executor-memory 2G --executor-cores 2 --master yarn --queue ai"

if [ ! ${dateEnd} ]; then
    dateEnd=${dateBegin}
fi

t1=`date -d "${dateBegin}" +%s`
t2=`date -d "${dateEnd}" +%s`

echo "ETLJob1 run from "${dateBegin}" to "${dateEnd}"..."

while [[ ${t2} -ge ${t1} ]]
do
   echo "Start run "${dateBegin}"..."
   logFile="./logs/etl_"${dateBegin}".log"
   spark-submit --class ${etlClass} ${sparkConf} ${etlJarName} ${dateBegin} > ${logFile} 2>&1

   dateBegin=$(date -d "${dateBegin} +1 days" "+%Y-%m-%d")
   t1=`date -d "${dateBegin}" +%s`
done

# bash run.sh 2018-09-03 2018-09-10 user
