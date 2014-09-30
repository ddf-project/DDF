#!/bin/bash

echo
echo "###########################i#########"
echo "# Stop pAnalytics server if running #"
echo "#####################################"
pgrep -fl adatao.bigr >/dev/null 2>&1
if [ "$?" == "0" ]; then
	echo -e '\t Stopping pAnalytics server ...'
	pkill -9 -f adatao.bigr >/dev/null 2>&1 || echo "Something wrong! pAnalytics server is running but could not be killed."
fi

pgrep -fl Rserve >/dev/null 2>&1
if [ "$?" == "0" ]; then
	echo -e '\t Stopping Rserve ...'
	pkill -9 -f Rserve >/dev/null 2>&1  || echo "Something wrong! Rserve is running but could not be killed."
fi

sleep 1
