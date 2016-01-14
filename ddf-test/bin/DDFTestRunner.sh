#!/bin/bash

# This is a program that tests a DDF-on-X implementation

function Input(){
echo ""
echo "Hello, welcome to ddf-test. This script will ask you choose an engine and run the tests."
echo ""
echo "Choose your java options for tests (for spark: -Dhive.metastore.warehouse.dir=/tmp/hive/warehouse )"
echo ""
echo -n "Enter your java options or leave blank and press [ENTER]: "
read DDF_OPTIONS
echo ""
echo -n "Enter your ddf-on-x jar's location(required) and press [ENTER]: "
read DDF_JARS
}

function Run(){
SBT_OPTS="-Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256M"

if [ "$DDF_OPTIONS" = "" ]
then
   java $SBT_OPTS -jar "./bin/sbt-launch.jar" \
   'set unmanagedJars in Test := ((file("'$DDF_JARS'") +++ file("./lib")) ** "*.jar").classpath' \
   'test'
else
   java $SBT_OPTS -jar "./bin/sbt-launch.jar" \
   'set unmanagedJars in Test := ((file("'$DDF_JARS'") +++ file("./lib")) ** "*.jar").classpath' \
   'set javaOptions in Test ++= Seq("'$DDF_OPTIONS'") ' \
   'test'
fi
}

function Main(){
Input
Run
}

Main