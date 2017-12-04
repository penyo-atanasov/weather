#!/bin/bash
for i in `cat pids`
do
	echo "Killing process..."
	ps aux | grep $i
	kill -9 $i
done

rm pids