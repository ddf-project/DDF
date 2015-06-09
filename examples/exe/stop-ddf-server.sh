#!/bin/bash

echo
echo "###########################i#########"
echo "# Stop DDF server if running #"
echo "#####################################"
pgrep -fl io.ddf.spark.examples >/dev/null 2>&1
if [ "$?" == "0" ]; then
	echo -e '\t Stopping DDF server ...'
	pkill -9 -f io.ddf.spark.examples >/dev/null 2>&1 || echo "Something wrong! DDF server is running but could not be killed."
fi

pgrep -fl Rserve >/dev/null 2>&1
if [ "$?" == "0" ]; then
	echo -e '\t Stopping Rserve ...'
	pkill -9 -f Rserve >/dev/null 2>&1  || echo "Something wrong! Rserve is running but could not be killed."
fi

sleep 1
