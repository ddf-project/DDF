#!/bin/bash -

#
# Define and export an environment variable
#
# @param $1 name
# @param $2 value - if empty, then variable is unset
# @return side effect of setting the variable
#
FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" 
export SPARK_HOME="$FWDIR/../exe"

function define {
  if [ "$2" != "" ] ; then
    export $1="$2"
    echo "$1=$2"
  else
    echo "Unsetting $1"
    unset $1
  fi
}

usage() {
        echo "
        Usage: ddf-env.sh
            [--cluster (default: mesos; other options is:
            yarn (for yarn cluster),
            spark (for distributed standalone spark),
            localspark (for local single-node standalone spark)]
        "
        exit 1
}
[[ "$1" == -h || "$1" == --help ]] && usage

cluster=mesos
do_parse_args() {
        while [[ -n "$1" ]] ; do
                case $1 in
                        "--cluster" )
                                shift ; cluster=$1
                                ;;
                esac
                shift
        done
}
do_parse_args $@

export DDF_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
#######################################################
# You need to define the following evn vars           #
#######################################################
export TMP_DIR=/tmp # this where pAnalytics server stores temporarily files
export LOG_DIR=/tmp # this where pAnalytics server stores log files
export SPARK_HOME=${DDF_HOME}/exe/
export RLIBS="${DDF_HOME}/rlibs"
export RSERVE_LIB_DIR="${RLIBS}/Rserve/libs/"
export DDF_CORE_JAR=`find ${DDF_HOME}/../core -name ddf_core_*.jar | grep -v '\-tests.jar'`
export DDF_SPARK_JAR=`find ${DDF_HOME} -name ddf_spark_*.jar | grep -v '\-tests.jar'`
echo DDF_CORE_JAR=$DDF_CORE_JAR
echo DDF_SPARK_JAR=$DDF_SPARK_JAR
SPARK_CLASSPATH=$DDF_CORE_JAR
SPARK_CLASSPATH+=:"$DDF_SPARK_JAR"
SPARK_CLASSPATH+=:"${DDF_HOME}/../lib_managed/jars/*"
SPARK_CLASSPATH+=:"${DDF_HOME}/../lib_managed/bundles/*"
SPARK_CLASSPATH+=:"${DDF_HOME}/../lib_managed/orbits/*"

#make sure you set HADOOP_CONF_DIR
#export HADOOP_CONF_DIR=
#export HIVE_CONF_DIR=

#The order of the following two lines is important please dont change
SPARK_CLASSPATH+=":${HIVE_CONF_DIR}"
[ "X$HADOOP_CONF_DIR" != "X" ] && SPARK_CLASSPATH+=":${HADOOP_CONF_DIR}"

if [[ -z "$SPARK_MEM" ]]; then
    . ${FWDIR}/exe/mem-size-detection.sh
fi
echo "SPARK_MEM = "$SPARK_MEM

SPARK_JAVA_OPTS="-Dspark.storage.memoryFraction=0.6"
SPARK_JAVA_OPTS+=" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
SPARK_JAVA_OPTS+=" -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=io.spark.content.KryoRegistrator"
SPARK_JAVA_OPTS+=" -Dlog4j.configuration=ddf-log4j.properties"
SPARK_JAVA_OPTS+=" -Dspark.local.dir=${TMP_DIR}"
SPARK_JAVA_OPTS+=" -Dspark.ui.port=30001"
SPARK_JAVA_OPTS+=" -Dspark.driver.port=20001"
SPARK_JAVA_OPTS+=" -Djava.io.tmpdir=${TMP_DIR}"
SPARK_JAVA_OPTS+=" -Dspark.kryoserializer.buffer.mb=125"
SPARK_JAVA_OPTS+=" -Dspark.executor.memory=${SPARK_MEM}"
export SPARK_JAVA_OPTS
if [ "X$cluster" == "Xyarn" ]
then
        echo "Running DDF with Yarn"
        export SPARK_MASTER="yarn-client"
        SPARK_CLASSPATH+=:"${DDF_HOME}/conf/"
        export SPARK_CLASSPATH
        export SPARK_WORKER_INSTANCES=2
        export SPARK_WORKER_CORES=8
        export SPARK_WORKER_MEMORY=$SPARK_MEM
        export SPARK_JAR=`find ${DDF_HOME}/ -name ddf_spark-assembly-*.jar`
        export HADOOP_NAMENODE=`cat ${HADOOP_CONF_DIR}/masters`
        export SPARK_YARN_APP_JAR=hdfs://${HADOOP_NAMENODE}:9000/user/root/ddf_spark-assembly-0.9.jar
        [ "X$SPARK_YARN_APP_JAR" == "X" ] && echo "Please define SPARK_YARN_APP_JAR" && exit 1
        [ "X$HADOOP_CONF_DIR" == "X" ] && echo "Please define HADOOP_CONF_DIR" && exit 1
        [ "X$SPARK_WORKER_INSTANCES" == "X" ] && echo "Notice! SPARK_WORKER_INSTANCES is not defined, the default value will be used instead"
        [ "X$SPARK_WORKER_CORES" == "X" ] && echo "Notice! SPARK_WORKER_CORES is not defined, the default value will be used instead"
elif [ "X$cluster" == "Xmesos" ] 
then
        echo "Running DDF with Mesos"
        #export SPARK_MASTER= #mesos://<host>:<port>
elif [ "X$cluster" == "Xspark" ]
then
        echo "Running DDF with Spark"
        #export SPARK_MASTER= #spark://<host>:<port>
elif [ "X$cluster" == "Xlocalspark" ]
then
        echo "Running DDF  with Spark in local"
        SPARK_CLASSPATH+=:"${DDF_HOME}/local/conf"
        export SPARK_CLASSPATH
        export SPARK_WORKER_MEMORY=$SPARK_MEM
        export SPARK_MASTER=local
fi

OUR_JAVA_OPTS="$SPARK_JAVA_OPTS"
# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi
SPARK_MEM=${SPARK_MEM:-512m}
export SPARK_MEM

JAVA_OPTS="$OUR_JAVA_OPTS"
JAVA_OPTS="$JAVA_OPTS -Djava.library.path=$SPARK_LIBRARY_PATH"
JAVA_OPTS="$JAVA_OPTS -Xms$SPARK_MEM -Xmx$SPARK_MEM"

# Load extra JAVA_OPTS from conf/java-opts, if it exists
if [ -e $FWDIR/conf/java-opts ] ; then
  JAVA_OPTS="$JAVA_OPTS `cat $FWDIR/conf/java-opts`"
fi
export JAVA_OPTS

TOOLS_DIR="$FWDIR"/tools
SPARK_TOOLS_JAR=""
if [ -e "$TOOLS_DIR"/target/scala-$SCALA_VERSION/*assembly*[0-9Tg].jar ]; then
  # Use the JAR from the SBT build
  export SPARK_TOOLS_JAR=`ls "$TOOLS_DIR"/target/scala-$SCALA_VERSION/*assembly*[0-9Tg].jar`
fi
if [ -e "$TOOLS_DIR"/target/spark-tools*[0-9Tg].jar ]; then
  # Use the JAR from the Maven build
  # TODO: this also needs to become an assembly!
  export SPARK_TOOLS_JAR=`ls "$TOOLS_DIR"/target/spark-tools*[0-9Tg].jar`
fi

export CLASSPATH="$SPARK_CLASSPATH"

