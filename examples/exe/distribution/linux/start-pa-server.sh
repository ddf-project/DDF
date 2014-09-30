#!/bin/bash

usage() {
	echo "
	Usage: start-pa-server 
		[--standalone-spark (default: No)]
		[--start-spark (default: No)]
	"
	exit 1
}

[[ "$1" == -h || "$1" == --help ]] && usage

do_parse_args() {
	while [[ -n "$1" ]] ; do
		case $1 in
			"--standalone-spark" )
				shift ; standalone_spark=1
				;;
				
			"--start-spark" )
				shift ; start_spark=1  
				;;
				
			* )
				usage "Unknown switch '$1'"
				;;
		esac
	done	
}

do_parse_args $@

cd `dirname $0`/../ >/dev/null 2>&1
DIR=`pwd`

paenv="${DIR}/conf/pa-env.sh"
[ ! -f $paenv ] && echo "Fatal: $paenv file does not exist" && exit 1

echo
echo "#################################################"
echo "# Export pAnalytics/Spark Environment Variables #"
echo "#################################################"
echo
source $paenv $@
echo "SPARK_MASTER="${SPARK_MASTER}
echo "SPARK_CLASSPATH="${SPARK_CLASSPATH}
echo "SPARK_JAVA_OPTS="${SPARK_JAVA_OPTS}

${DIR}/exe/stop-pa-server.sh

[ "X$start_spark" == "X1" ] && ${DIR}/exe/start-spark-cluster.sh $@

# Start Rserve
${DIR}/exe/start-rserve.sh
 
echo
echo "###########################"
echo "# Start pAnalytics server #"
echo "###########################"
nohup ${DIR}/exe/run -Dbigr.multiuser=false -Dlog.dir=${LOG_DIR} adatao.bigr.thrift.Server $PA_PORT >${LOG_DIR}/pa.out 2>&1 &
echo

sleep 5
pgrep -fl adatao.bigr.thrift.Server >/dev/null 2>&1 || echo "Error: No 'Server' Java process found. Something is wrong"

