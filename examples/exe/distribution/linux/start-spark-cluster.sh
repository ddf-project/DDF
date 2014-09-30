#!/bin/bash



DIR="$(cd `dirname $0`/../ 2>&1 >/dev/null; echo $PWD)"
paenv="$DIR/conf/pa-env.sh" ; source $paenv --standalone-spark

${DIR}/exe/stop-spark-cluster.sh

echo
echo "#############################" 
echo "# Start Spark cluster #"
echo "#############################"
nohup ${DIR}/exe/run spark.deploy.master.Master --ip ${SPARK_HOST} --port ${SPARK_PORT} >${TMP_DIR}/spark-master.out 2>&1 &
nohup ${DIR}/exe/run spark.deploy.worker.Worker ${SPARK_MASTER} >${TMP_DIR}/spark-worker.out 2>&1 &

sleep 10

pgrep -f spark.deploy.master.Master >/dev/null 2>&1 || echo "Error: No 'Master' Java process found. Something is wrong"
pgrep -f spark.deploy.worker.Worker >/dev/null 2>&1 || echo "Error: No 'Worker' Java process found. Something is wrong"

