#!/usr/bin/env bash
#
# This file should only contain simple environment variable assignments
#

export SCALA_VERSION=2.10.3

# Either partial path above working directory, or full path
[ -z $DDF_INI ] && export DDF_INI=ddf-conf/ddf.ini

[ -z $HADOOP_HOME] && export HADOOP_HOME=/root/hadoop
[ -z $HIVE_HOME] && export HIVE_HOME=/root/hive

export JAVA_OPTS+="-Dderby.stream.error.file=/tmp/hive/derby.log"
