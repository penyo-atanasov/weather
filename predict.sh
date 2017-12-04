#!/bin/bash
parent="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $parent

source spark_version
export SPARK_HOME=$parent/$spark_dir
spark_submit=$parent/$spark_dir/bin/spark-submit
version=0.0.1
mkdir -p logs
rm -r logs/* 2> /dev/null

#start spark learning job to get the predicted results.
nohup $spark_submit --class com.cba.weather.spark.ml.LearningJob spark/target/spark-$version-jar-with-dependencies.jar  > logs/learning.log &
echo $! >> pids
