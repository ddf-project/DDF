#!/bin/bash

PKG_NAME=bigr-server_2.9.3-0.8.1
PA_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
cd ${PA_HOME} >/dev/null 2>&1
mvn assembly:single
rm -rf /tmp/${PKG_NAME}/
tar zxvf ${PA_HOME}/target/scala-2.9.3/${PKG_NAME}-bin.tar.gz -C /tmp/

cd /tmp/${PKG_NAME}/
[ "$?" -eq 0 ] || exit 1

exe/start-pa-server.sh --standalone-spark --start-spark

sleep 30
Rscript examples/R/income.R
Rscript examples/R/mtcars.R

cd - >/dev/null 2>&1