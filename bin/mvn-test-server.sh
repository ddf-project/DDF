#!/bin/bash -x
TEST=$1

cd "`dirname $0`"/../server

if [ "$TEST" = "" ] ; then
  mvn test
else
  mvn test -Dtest=$TEST
fi
