#!/bin/bash
#
# Run once to set up a new dev environment
#

PROJECTS="core spark contrib examples"
ME=`basename $0 .sh`
DIR="`dirname $0`"
cd "$DIR/.."

SBT=bin/sbt
MVN=bin/mvn-color.sh

function Run() {
  echo "
  ***************************************************************************************
  Running one time 'mvn install -N -DskipTests' under root to install root pom.xml
  ***************************************************************************************
  "
  ($MVN install -N -DskipTests) || Error "mvn install in root failed"


  for project in $PROJECTS ; do
    echo "
  ***************************************************************************************
  Running one time 'mvn clean package install -DskipTests' under '$project/'
  ***************************************************************************************
    "
    (cd $project ; ../$MVN clean package install -DskipTests) || Error "mvn package in $project failed"
  done
}

function Error() {
  local MSG=$1
  echo "
  ***************************************************************************************
  Error: ${MSG:=Build error}

  NB: If you see something like 'Invalid target release', be sure you have JAVA_HOME set
  ***************************************************************************************
  "
  exit 1
}

Run
