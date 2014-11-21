
FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" 
export SPARK_HOME="$FWDIR/../../exe"
export DDF_HOME="$FWDIR/../.."

SPARK_JAVA_OPTS+=" -Dlog4j.configuration=ddf-local-log4j.properties"
SPARK_CLASSPATH+=:"${DDF_HOME}/conf/local"
export SPARK_MASTER=local

#if [[ -z "$SPARK_MEM" ]]; then
#    . ${DDF_HOME}/exe/mem-size-detection.sh
#fi
#echo "SPARK_MEM = "$SPARK_MEM

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
