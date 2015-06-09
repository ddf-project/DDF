#!/bin/bash

echo
echo "############################"
echo "# About to starting Rserve #"
echo "############################"
[ "X$RLIBS" != "X" ] || { echo "RLIBS is undefined"; exit 1; }
[ "X$TMP_DIR" != "X" ] || { echo "TMP_DIR is undefined"; exit 1; }
[ "X$LOG_DIR" != "X" ] || { echo "LOG_DIR is undefined"; exit 1; }

echo -e '\t Checking if Rserve is installed'
if [ ! -f ${RSERVE_LIB_DIR}/Rserve ]; then
	echo -e '\t Rserve has not been installed yet. Install it now ...'
	if [ ! -d ${RLIBS} ]; then
    	mkdir -p ${RLIBS}
		[ "$?" -eq "0" ] || { echo "Failed to create dir ${RSERVE_LIB_DIR}"; exit 1; }
	fi
	R -q -e "install.packages('Rserve',lib='${RLIBS}',contriburl=contrib.url('http://cran.cnr.Berkeley.edu/'))" >${LOG_DIR}/Rserve.log 2>&1
	[ "$?" -eq "0" ] || { echo "Failed to install Rserve"; exit 1; } 
fi 
echo -e '\t Starting Rserve ...'
nohup R CMD ${RSERVE_LIB_DIR}/Rserve --vanilla --RS-workdir ${TMP_DIR}/Rserv/ --RS-encoding utf8 >>${LOG_DIR}/Rserve.log 2>&1 &

sleep 5
pgrep -fl Rserve >/dev/null 2>&1 || echo "Error: No 'Rserve' process found. Something is wrong"
