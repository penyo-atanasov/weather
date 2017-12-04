#!/bin/bash
parent="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $parent
file="pids"
if [ -f $file ]; then
	echo "Processes are still running. Use stop.sh before starting new ones." && exit 0
fi

source spark_version
export SPARK_HOME=$parent/$spark_dir
spark_submit=$parent/$spark_dir/bin/spark-submit
version=0.0.1
mkdir -p logs
rm -r logs/* 2> /dev/null
#start SocketServer to feed json data for Spark
nohup python datafeed/datafeeder.py conf/iata.txt data/predictions/test.csv >> logs/datafeed.log &
echo $! >> pids
#start spark streaming to aggregate the data into parquet files
nohup $spark_submit --class com.cba.weather.spark.stream.SocketStreamingJob spark/target/spark-$version-jar-with-dependencies.jar  > logs/streaming.log &
echo $! >> pids
