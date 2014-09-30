#!/bin/bash

function Usage {
	echo "Start Spark local cluster using configuration"
	echo "Usage: $0 [additional args]"
	exit 1
}

protocol=spark host=localhost port=7077 webui_port=9080
export SPARK_MASTER_FORCED="$protocol://$host:$port" # this will force bigr-env.sh to use this for SPARK_MASTER

DIR="$(cd `dirname $0` 2>&1 >/dev/null; echo $PWD)"
bigrenv="$DIR/../conf/bigr-env.sh" ; source $bigrenv --local $@


echo ""
echo "######## SCALA_HOME is $SCALA_HOME. Make sure it is version compatible (e.g., 2.9.2) ########"
echo ""

$DIR/stop-local-spark-cluster.sh


cd "$DIR" 2>&1 >/dev/null

echo "" ; echo "########"
echo $SPARK_HOME/run spark.deploy.master.Master --ip $host --port $port --webui-port $webui_port
nohup $SPARK_HOME/run spark.deploy.master.Master --ip $host --port $port --webui-port $webui_port &
echo "" ; echo "########"
echo $SPARK_HOME/run spark.deploy.worker.Worker $SPARK_MASTER --webui-port $((webui_port+1))
nohup $SPARK_HOME/run spark.deploy.worker.Worker $SPARK_MASTER --webui-port $((webui_port+1)) &

sleep 2

jps | grep -v Master >/dev/null ; [ $? == 0 ] || echo "Error: No 'Master' Java process found. Something is wrong"
jps | grep -v Worker >/dev/null ; [ $? == 0 ] || echo "Error: No 'Worker' Java process found. Something is wrong"
