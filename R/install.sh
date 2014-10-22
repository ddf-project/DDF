#!/bin/bash

# Install development version of R package
FWDIR="$(cd `dirname $0`; pwd)"

# Copy jar files to package's inst/java directory
SCALA_VERSION="2.10"
SPARK_VERSION="0.9"
TARGET_DIR="$FWDIR/../spark/target/scala-$SCALA_VERSION"

if [ ! -f "$TARGET_DIR/ddf_spark-assembly-$SPARK_VERSION.jar" ]; then
  echo "You must build DDF's Java core and spark package firstly" 
  echo "Guide: Go to $(cd $FWDIR/../; pwd) and run this command"
  echo "mvn package install [-DskipTests]"
  exit -1
fi

rm -r package/inst/java/* || mkdir package/inst/java
cp $TARGET_DIR/ddf_spark-assembly-$SPARK_VERSION.jar package/inst/java/

# Install R
R CMD INSTALL package/
