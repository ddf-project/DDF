#!/bin/bash

pgrep -fl spark.deploy.master.Master >/dev/null 2>&1
if [ "$?" == "0" ]; then
	pkill -f spark.deploy.master.Master >/dev/null 2>&1
fi 

pgrep -fl spark.deploy.worker.Worker >/dev/null 2>&1
if [ "$?" == "0" ]; then 
	pkill -f spark.deploy.worker.Worker >/dev/null 2>&1
fi 
