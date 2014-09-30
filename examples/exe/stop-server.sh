#!/bin/bash
while true ; do
	pgrep -fl adatao.bigr || exit 0
	pkill -f adatao.bigr
  sleep 1
done
