#!/bin/bash
#${DIR}/exe/start-rserve.sh

usage() {
    echo "
    Usage: start-DDF-server
        [--cluster (default: mesos; other options is:
                            yarn (for yarn cluster),
                            spark (for distributed standalone spark),
                            localspark (for local single-node standalone spark)]
        [--start-spark (default: No)]
    "
    exit 1
}

[[ "$1" == -h || "$1" == --help ]] && usage

cluster=mesos

do_parse_args() {
    while [[ -n "$1" ]] ; do
        case $1 in
                "--cluster" )
                    shift ; cluster=$1 ; shift
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

ddfenv="${DIR}/../spark/conf/ddf-env.sh"
[ ! -f $ddfenv ] && echo "Fatal: $ddfenv file does not exist" && exit 1

echo
echo "#################################################"
echo "# Export DDF Server/Spark Environment Variables #"
echo "#################################################"
echo
source $ddfenv $@ --cluster $cluster
echo "SPARK_MASTER="${SPARK_MASTER}
echo "SPARK_CLASSPATH="${SPARK_CLASSPATH}
echo "SPARK_JAVA_OPTS="${SPARK_JAVA_OPTS}

mkdir -p ${LOG_DIR}

${DIR}/exe/stop-ddf-server.sh

if [[ -z "$SPARK_MEM" ]]; then
	. ${DIR}/exe/mem-size-detection.sh
fi
echo "SPARK_MEM = "$SPARK_MEM

[ "X$start_spark" == "X1" ] && ${DIR}/exe/start-spark-cluster.sh $@


echo
echo "###########################"
echo "# Start DDF server #"
echo "###########################"
nohup ${DIR}/exe/spark-class -Dpa.security=false -Dbigr.multiuser=false -Dlog.dir=${LOG_DIR} io.ddf.spark.examples.rest.Server  > ddf.out 2>&1 &
echo

sleep 5
pgrep -fl io.ddf.spark.examples.rest.Server >/dev/null 2>&1 || echo "Error: No 'Server' Java process found. Something is wrong"

