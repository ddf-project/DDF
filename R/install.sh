#!/bin/bash

# Install development version of R package
FWDIR="$(cd `dirname $0`; pwd)"

# Copy jar files to package's inst/java directory
SCALA_VERSION="2.10"
SPARK_VERSION="1.2"
DDF_VERSION="1.2-adatao"
TARGET_DIR="$FWDIR/../spark/target/scala-$SCALA_VERSION"

if [ ! -f "$TARGET_DIR/ddf_spark_$SCALA_VERSION-$DDF_VERSION.jar" ]; then
  echo "You must build DDF's Java core and spark package firstly" 
  echo "Guide: Go to $(cd $FWDIR/../; pwd) and run this command"
  echo "mvn package install [-DskipTests]"
  exit -1
fi

rm -r package/inst/java/* || mkdir package/inst/java
cp $TARGET_DIR/ddf_spark_$SCALA_VERSION-$DDF_VERSION.jar package/inst/java/
cp $TARGET_DIR/lib/*.jar package/inst/java/

# log4j
rm -r package/inst/conf/local/* || mkdir -p package/inst/conf/local
cp $FWDIR/../core/conf/local/ddf-local-log4j.properties package/inst/conf/local/ddf-local-log4j.properties

# Install R
export JAVA_HOME=${JAVA_HOME:-`readlink -f /usr/bin/javac | sed "s:/bin/javac::"`}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${JAVA_HOME}/jre/lib/amd64/server
R CMD INSTALL package/
