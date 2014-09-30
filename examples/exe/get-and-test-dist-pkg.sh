#!/bin/bash

PKG_NAME=bigr-server_2.9.3-0.8.1
FULL_PKG_NAME=${PKG_NAME}-bin.tar.gz

PA_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
rm -rf /tmp/${FULL_PKG_NAME}
python ${PA_HOME}/../Installer/adatao-s3.py get -b pa-dist -s ${FULL_PKG_NAME} -l /tmp/${FULL_PKG_NAME}
rm -rf /tmp/${PKG_NAME}
tar zxvf /tmp/${FULL_PKG_NAME} -C /tmp/

cd /tmp/${PKG_NAME}/
[ "$?" -eq 0 ] || exit 1

exe/start-pa-server.sh --standalone-spark --start-spark
sleep 30
Rscript examples/R/income.R
Rscript examples/R/mtcars.R

cd - >/dev/null 2>&1