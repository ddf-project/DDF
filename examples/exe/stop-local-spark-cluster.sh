#!/bin/bash
while true ; do
	PID=`jps | egrep "Master|Worker" | head -1 | cut -d' ' -f1`
	[ "X$PID" == "X" ] && exit 0
	echo -n "Killing "
	ps www $PID | tail -1
	kill $PID
	sleep 1
done
